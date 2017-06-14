/*
 * Copyright (c) 2016 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.cbio.portal.pipelines.importer.config.processor;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.GeneJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.model.ProfileDataRecord;
import org.cbio.portal.pipelines.importer.util.*;

import java.util.*;
import java.util.regex.*;
import org.apache.commons.logging.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class ProfileDataProcessor implements ItemProcessor<ProfileDataRecord, ProfileDataRecord> {
    
    @Autowired
    GeneJdbcDaoImpl geneJdbcDaoImpl;
    
    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(ProfileDataProcessor.class);
    
    @Override
    public ProfileDataRecord process(ProfileDataRecord profileDataRecord) throws Exception {
        ProfileDataRecord screenedRecord = screenProfileDataRecord(profileDataRecord);
        if (screenedRecord != null && profileDataRecord.isCnaData()) {
            List<CnaEvent> cnaEvents = loadCnaEvents(screenedRecord);
            screenedRecord.setCnaEvents(cnaEvents);
        }

        return screenedRecord;
    }
    
    /**
     * Performs base data screening to determine whether profile record should be skipped or not.
     * Returns null if gene information cannot be resolve from record. 
     * 
     * @param profileDataRecord
     * @param caseIdsMap
     * @param isRppaProfile
     * @return CompositeProfileData
     */
    private ProfileDataRecord screenProfileDataRecord(ProfileDataRecord profileDataRecord) {
        // check that entrez gene id and hugo gene symbol are not both invalid
        String hugoGeneSymbol = profileDataRecord.isRppaProfile()?profileDataRecord.getCompositeElementRef():profileDataRecord.getHugoSymbol();
        Integer entrezGeneId = !DataFileUtils.isNullOrEmptyValue(profileDataRecord.getEntrezGeneId())?
                Integer.valueOf(profileDataRecord.getEntrezGeneId()):null;
        if ((DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol) || hugoGeneSymbol.equalsIgnoreCase("unknown")) 
            && entrezGeneId == null) {
            LOG.warn("Skipping entry with invalid (Entrez_Gene_Id,Hugo_Symbol): " + 
                    "(" + profileDataRecord.getEntrezGeneId() + "," + (profileDataRecord.getHugoSymbol()) + ")");
            return null;
        }
        
        // ignore entries with gene symbols separated by /// or gene symbols specified as ---
        if (!DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol) && 
                (hugoGeneSymbol.contains("///") || hugoGeneSymbol.contains("---"))) {
            if (hugoGeneSymbol.contains("///")) {
                LOG.warn("Skipping entry with information for multiple genes: " + hugoGeneSymbol);
            }
            else {
                LOG.warn("Skipping entry with unknown gene information: " + hugoGeneSymbol);
            }
            return null;
        }
        
        // generate list of genes (potentially multiple genes for microRNA or RPPA profile)
        List<Gene> geneList = new ArrayList();
        if (profileDataRecord.isRppaProfile()) {
            // composite element ref value cannot be empty for rppa profile
            if (DataFileUtils.isNullOrEmptyValue(profileDataRecord.getCompositeElementRef())) {
                LOG.warn("Ignoring line with no Composite.Element.Ref value");
                return null;
            }
            
            // try to parse composite element ref value
            hugoGeneSymbol = profileDataRecord.getCompositeElementRef();
            String[] parts = hugoGeneSymbol.split("\\|");
            if (parts.length < 2) {
                LOG.warn("Could not parse Composite.Element.Ref value: " + hugoGeneSymbol);
                return null;
            }
            
            // skip entries with array id's that have already been loaded
            String[] geneSymbols = parts[0].split(" ");
            String arrayId = parts[1];
            profileDataRecord.setArrayId(arrayId);

            // generate list of genes from gene symbols
            geneList = geneDataUtils.resolveGeneFromCompositeElementRef(geneSymbols);
            if (geneList.isEmpty()) {
                LOG.warn("Gene symbols could not be resolved for Composite.Element.Ref: " + hugoGeneSymbol);
                return null;
            }

            // get list of phospho genes from array id
            Pattern p = Pattern.compile("(p[STY][0-9]+(?:_[STY][0-9]+)*)");
            Matcher m = p.matcher(arrayId);
            String residue;
            if (m.find()) {
                // import phospho genes
                residue = m.group(1);
                geneList = importPhosphoGene(geneList, residue);
            }
        }
        else {
            // check if gene can be resolved from the entrez gene id and/or hugo gene symbol
            if (!DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol)) {
                hugoGeneSymbol = hugoGeneSymbol.split("\\|")[0]; // handle cases where gene symbol includes accession
            }
            Gene gene = geneDataUtils.resolveGeneFromRecordData(entrezGeneId, hugoGeneSymbol, null);
            if (gene == null) {
                LOG.warn("Could not resolve gene from (Entrez_Gene_Id,Hugo_Symbol): " + 
                        "(" + entrezGeneId + "," + hugoGeneSymbol + ")");
                return null;
            }
            
            // if entrez gene id doesn't match the resolved entrez gene id then 
            // switch the resolved entrez gene id to the value given by the file,
            // otherwise set entrez gene id to the resolved value
            if (entrezGeneId != null) {
                // only update resolved entrez gene id if doesn't match value in file
                if (!entrezGeneId.equals(gene.getEntrezGeneId())) {
                    gene.setEntrezGeneId(entrezGeneId);
                }
            }
            else {
                entrezGeneId = gene.getEntrezGeneId();
            }
            
            // if hugo symbol doesn't match the resolved hugo symbol then 
            // switch the resolved hugo symbol to the value given by the file,
            // otherwise set hugo symbol to the resolved value
            if (!DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol)) {
                // only update resolved hugo symbol if doesn't match value in file
                if (!hugoGeneSymbol.equals(gene.getHugoGeneSymbol())) {
                    gene.setHugoGeneSymbol(hugoGeneSymbol);
                }
            }
            else {
                hugoGeneSymbol = gene.getHugoGeneSymbol();
            }
            // add gene to geneList to generate composite profile data
            geneList.add(gene);
        }
        
        // check whether gene list is empty and, if so, log appropriate warning
        if (geneList.isEmpty()) {
            if (!DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol) && !hugoGeneSymbol.toLowerCase().contains("-mir-")) {
                LOG.warn("Skipping entry with unknown microRNA: " + hugoGeneSymbol);
                return null;
            }
            else {
                LOG.warn("Could not resolve gene from (Entrez_Gene_Id,Hugo_Symbol): " + 
                        "(" + entrezGeneId + "," + hugoGeneSymbol + ")");
                return null;
            }
        }
        // update gene list for profile data record
        profileDataRecord.setEntrezGeneId(String.valueOf(geneList.get(0).getEntrezGeneId()));
        profileDataRecord.setHugoSymbol(geneList.get(0).getHugoGeneSymbol());
        profileDataRecord.setCompositeGeneList(geneList);

        return profileDataRecord;
    }
    
    /**
     * Returns a list of phospho genes and adds phospho genes to db if not already exists.
     * 
     * @param genes
     * @param residue
     * @return List<Gene>
     */
    private List<Gene> importPhosphoGene(List<Gene> genes, String residue) {
        List<Gene> phosphoGenes = new ArrayList();
        
        // for each gene, find equivalent phospho gene and add to db if not already exists
        for (Gene gene : genes) {
            String phosphoGeneSymbol = gene.getHugoGeneSymbol().toUpperCase() + "_" + residue;
            Gene existingPhosphoGene = geneJdbcDaoImpl.getGene(phosphoGeneSymbol);
            if (existingPhosphoGene == null) {
                // create a new phospho gene instance and add to db
                Gene newPhosphoGene = geneJdbcDaoImpl.addGene(geneDataUtils.createPhosphoGene(gene, residue));
                phosphoGenes.add(newPhosphoGene);
            }
            else {
                phosphoGenes.add(existingPhosphoGene);
            }
        }
        
        return phosphoGenes;
    }
    
    /**
     * Load CNA events for a given profile data record object. 
     * 
     * @param profileDataRecord
     * @return List<CnaEvent>
     */
    private List<CnaEvent> loadCnaEvents(ProfileDataRecord profileDataRecord) {
        Map<Integer, String> caseProfileDataRecords = profileDataRecord.getCaseProfileDataMap();
        // generate list of cna events
        List<CnaEvent> cnaEvents = new ArrayList();
        for (Integer sampleId : caseProfileDataRecords.keySet()) {
            // change partial deletion to full deletion
            String alteration = caseProfileDataRecords.get(sampleId);
            if (alteration.equals(CnaEvent.AlterationType.PARTIAL_DELETION.getName())) {
                alteration = CnaEvent.AlterationType.HOMOZYGOUS_DELETION.getName();
            }
            
            // skip cna events that are not homozygous deletions or amplifications
            if (!alteration.equals(CnaEvent.AlterationType.HOMOZYGOUS_DELETION.getName()) &&
                    !alteration.equals(CnaEvent.AlterationType.AMPLIFICATION)) {
                continue;
            }
            CnaEvent.AlterationType alterationType = alteration.equals(CnaEvent.AlterationType.HOMOZYGOUS_DELETION.getName())?
                    CnaEvent.AlterationType.HOMOZYGOUS_DELETION:CnaEvent.AlterationType.AMPLIFICATION;
            
            // create sample cna event and cna event
            SampleCnaEvent sampleCnaEvent = new SampleCnaEvent();
            sampleCnaEvent.setSampleId(sampleId);
            sampleCnaEvent.setGeneticProfileId(profileDataRecord.getGeneticProfileId());

            CnaEvent cnaEvent = new CnaEvent();
            cnaEvent.setEntrezGeneId(Integer.valueOf(profileDataRecord.getEntrezGeneId()));
            cnaEvent.setAlterationType(alterationType);
            cnaEvent.setSampleCnaEvent(sampleCnaEvent);
            
            // add cna event to list
            cnaEvents.add(cnaEvent);
        }
        
        return cnaEvents;
    }
    
}

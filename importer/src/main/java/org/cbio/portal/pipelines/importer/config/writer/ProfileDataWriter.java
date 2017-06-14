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

package org.cbio.portal.pipelines.importer.config.writer;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.*;
import org.cbio.portal.pipelines.importer.model.ProfileDataRecord;
import org.cbio.portal.pipelines.importer.config.composite.CompositeProfileData;
import org.cbio.portal.pipelines.importer.util.GeneDataUtils;

import java.util.*;
import org.apache.commons.logging.*;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class ProfileDataWriter implements ItemStreamWriter<ProfileDataRecord> {

    @Autowired
    GeneticAlterationJdbcDaoImpl geneticAlterationJdbcDaoImpl;

    @Autowired
    CnaEventJdbcDaoImpl cnaEventJdbcDaoImpl;

    @Autowired
    GeneDataUtils geneDataUtils;
    
    private Integer nextCnaEventId;
    private GeneticProfile geneticProfile;
    
    private boolean isCnaData;
    private int cnaEventCount;
    private int sampleCnaEventCount;
    private int geneticAlterationCount;
    private int additionalEntriesSkipped;
    private int validExtraRecords;
    private int skippedExtraRecords;
    
    private final Set<String> arrayIdSet = new HashSet<>();
    private final Set<Integer> entrezGeneIdSet = new HashSet<>();
    
    private static final Log LOG = LogFactory.getLog(ProfileDataWriter.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.nextCnaEventId = cnaEventJdbcDaoImpl.getLargestCnaEventId();
        this.isCnaData = (boolean) executionContext.get("isCnaData");
        this.geneticProfile = (GeneticProfile) executionContext.get("geneticProfile");
        LOG.info("Beginning profile data batch import for genetic profile: " + geneticProfile.getStableId());
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // add counts to execution context for step listener
        executionContext.put("geneticAlterationCount", geneticAlterationCount);
        executionContext.put("validExtraRecords", validExtraRecords);
        executionContext.put("skippedExtraRecords", skippedExtraRecords);
        executionContext.put("additionalEntriesSkipped", additionalEntriesSkipped);
        executionContext.put("totalGeneCount", entrezGeneIdSet.size());
        if (isCnaData) {
            executionContext.put("cnaEventCount", cnaEventCount);
            executionContext.put("sampleCnaEventCount", sampleCnaEventCount);
        }
    }
    
    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends ProfileDataRecord> list) throws Exception {
        List<CompositeProfileData> compositeProfileDataList = new ArrayList();

        // go through records and generate list of composite profile data
        for (ProfileDataRecord pdr : list) {
            // skip records with array ids that have already been added
            if (pdr.isRppaProfile() && !arrayIdSet.add(pdr.getArrayId())) {
                LOG.warn("Array Id found to be duplicated: " + pdr.getArrayId() + ". Record will be skipped");
                this.additionalEntriesSkipped++;
                continue;
            }
            
            // if cna data then only create one composite profile data object
            // and load cna events for the profile data record
            if (isCnaData) {
                Gene gene = pdr.getCompositeGeneList().get(0);
                
                // skip entry if gene has already been loaded from datafile 
                if (!entrezGeneIdSet.add(gene.getEntrezGeneId())) {
                    LOG.warn("Skipping entry since data has already been loaded for gene: " + gene.getHugoGeneSymbol());
                    this.additionalEntriesSkipped++;
                    continue;
                }
                CompositeProfileData cpd = new CompositeProfileData();
                cpd.setGene(gene);
                cpd.setGeneticProfile(geneticProfile);
                cpd.setCaseProfileDataRecords(pdr.getCaseProfileDataMap());
                cpd.setProfileCnaEvents(pdr.getCnaEvents());
                compositeProfileDataList.add(cpd);
            }
            else {
                // if not cna data then generate list of composite profile data for each gene in list
                List<CompositeProfileData> compositeProfileData = new ArrayList();
                List<Gene> geneList = pdr.getCompositeGeneList();
                for (int i=0; i< geneList.size(); i++) {
                    Gene gene = geneList.get(i);
                    // skip entry if gene has already been loaded from datafile 
                    if (!entrezGeneIdSet.add(gene.getEntrezGeneId())) {
                        LOG.warn("Skipping entry since data has already been loaded for gene: " + gene.getHugoGeneSymbol());
                        this.additionalEntriesSkipped++;
                        continue;
                    }

                    // create new composite profile data instance for current gene
                    CompositeProfileData cpd = new CompositeProfileData();
                    cpd.setGene(gene);
                    cpd.setGeneticProfile(geneticProfile);
                    cpd.setCaseProfileDataRecords(pdr.getCaseProfileDataMap());
                    if (i == 0) {
                        // always add first composite profile data record in list
                        compositeProfileData.add(cpd);
                    }
                    else {
                        // only add extra composite profile data records if microRNA or if RPPA profile
                        if (gene.getType().equals(geneDataUtils.MIRNA_TYPE) || pdr.isRppaProfile()) {
                            compositeProfileData.add(cpd);
                            this.validExtraRecords++;
                        }
                        else {
                            LOG.warn("Skipping ambiguous gene symbol: " + gene.getHugoGeneSymbol());
                            this.skippedExtraRecords++;
                        }
                    }
                    compositeProfileDataList.addAll(compositeProfileData);               
                }
            }
        }
        
        // import genetic alteration data and cna event data if any
        for (CompositeProfileData cpd : compositeProfileDataList) {
            // first add genetic alteration record
            geneticAlterationJdbcDaoImpl.addGeneticAlterations(cpd.getGeneticProfile(), cpd.getGene(), 
                    new ArrayList(cpd.getCaseProfileDataRecords().values()));
            this.geneticAlterationCount++;
            if (!isCnaData) {
                continue;
            }
            
            // import cna events in composite profile data if not null or empty
            if (cpd.getProfileCnaEvents().isEmpty()) {
                continue;
            }
            for (CnaEvent cnaEvent : cpd.getProfileCnaEvents()) {
                SampleCnaEvent sampleCnaEvent = cnaEvent.getSampleCnaEvent();

                // check if cna event already exists
                CnaEvent existingCnaEvent = cnaEventJdbcDaoImpl.getCnaEvent(cnaEvent);                
                if (existingCnaEvent != null) {
                    // if cna event already exists then update cna event id and import only the sample cna event
                    sampleCnaEvent.setCnaEventId(existingCnaEvent.getCnaEventId());
                    cnaEventJdbcDaoImpl.addSampleCnaEvent(sampleCnaEvent);
                    this.sampleCnaEventCount++;
                }
                else {
                    // update cna event id with next cna event id and import both the cna event and sample cna event                    
                    cnaEvent.setCnaEventId(++nextCnaEventId);
                    cnaEventJdbcDaoImpl.addCnaEvent(cnaEvent);

                    sampleCnaEvent.setCnaEventId(cnaEvent.getCnaEventId());
                    cnaEventJdbcDaoImpl.addSampleCnaEvent(sampleCnaEvent);
                    this.cnaEventCount++;
                    this.sampleCnaEventCount++;
                }
            }
        }
    }
    
}

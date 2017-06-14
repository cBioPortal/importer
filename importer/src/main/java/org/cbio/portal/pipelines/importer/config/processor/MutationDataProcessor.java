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
import org.mskcc.cbio.persistence.jdbc.*;
import org.cbio.portal.pipelines.importer.model.*;
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.config.composite.CompositeMutationData;

import com.google.common.base.Strings;
import org.apache.commons.logging.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutationDataProcessor implements ItemProcessor<MafRecord, CompositeMutationData> {

    @Autowired
    GeneDataUtils geneDataUtils;
    
    @Autowired
    UniProtIdMappingJdbcDaoImpl uniProtIdMappingJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(MutationDataProcessor.class);
        
    @Override
    public CompositeMutationData process(MafRecord mafRecord) throws Exception {
        // screen maf record and return null of record does not pass screening
        MafRecord screenedRecord = screenMafRecord(mafRecord);
        if (screenedRecord == null) {
            return null;
        }
        
        // process the maf record and extract mutation and mutation event data from processed record
        MafRecord processedRecord = processMafRecordData(screenedRecord);
        Mutation mutation = transformMafRecordToMutation(processedRecord);
        MutationEvent mutationEvent = transformMafRecordToMutationEvent(processedRecord);
        mutation.setMutationEvent(mutationEvent);
        
        // create instance of composite mutation data
        CompositeMutationData cmd = new CompositeMutationData();
        cmd.setMutation(mutation);
        
        return cmd;
    }
    
    /**
     * Transform a MAF record to a mutation instance.
     * 
     * @param mafRecord
     * @return Mutation
     */
    private Mutation transformMafRecordToMutation(MafRecord mafRecord) {
        Mutation mutation = new Mutation();
        mutation.setSampleId(mafRecord.getSampleId());
        mutation.setGeneticProfileId(mafRecord.getGeneticProfileId());
        mutation.setEntrezGeneId(Integer.valueOf(mafRecord.getEntrezGeneId()));
        mutation.setCenter(mafRecord.getCenter());
        mutation.setSequencer(mafRecord.getSequencer());
        mutation.setMutationStatus(mafRecord.getMutationStatus());
        mutation.setValidationStatus(mafRecord.getValidationStatus());
        mutation.setTumorSeqAllele1(mafRecord.getTumorSeqAllele1());
        mutation.setTumorSeqAllele2(mafRecord.getTumorSeqAllele2());
        mutation.setMatchedNormSampleBarcode(mafRecord.getMatchedNormSampleBarcode());
        mutation.setMatchNormSeqAllele1(mafRecord.getMatchNormSeqAllele1());
        mutation.setMatchNormSeqAllele2(mafRecord.getMatchNormSeqAllele2());
        mutation.setTumorValidationAllele1(mafRecord.getTumorValidationAllele1());
        mutation.setMatchNormValidationAllele1(mafRecord.getMatchNormValidationAllele1());
        mutation.setMatchNormValidationAllele2(mafRecord.getMatchNormValidationAllele2());
        mutation.setVerificationStatus(mafRecord.getVerificationStatus());
        mutation.setSequencingPhase(mafRecord.getSequencingPhase());
        mutation.setSequenceSource(mafRecord.getSequenceSource());
        mutation.setValidationMethod(mafRecord.getValidationMethod());
        mutation.setScore(mafRecord.getScore());
        mutation.setBamFile(mafRecord.getBamFile());
        mutation.setTumorAltCount(Integer.valueOf(mafRecord.gettAltCount()));
        mutation.setTumorRefCount(Integer.valueOf(mafRecord.gettRefCount()));
        mutation.setNormalAltCount(Integer.valueOf(mafRecord.getnAltCount()));
        mutation.setNormalRefCount(Integer.valueOf(mafRecord.getnRefCount()));
        
        return mutation;
    }
    
    /**
     * Transform a MAF record to a mutation event instance.
     * 
     * @param mafRecord
     * @return MutationEvent
     */
    private MutationEvent transformMafRecordToMutationEvent(MafRecord mafRecord) {
        MutationEvent mutationEvent = new MutationEvent();
        mutationEvent.setEntrezGeneId(Integer.valueOf(mafRecord.getEntrezGeneId()));
        mutationEvent.setChr(mafRecord.getChromosome());
        mutationEvent.setStartPosition(Long.valueOf(mafRecord.getStartPosition()));
        mutationEvent.setEndPosition(Long.valueOf(mafRecord.getEndPosition()));
        mutationEvent.setReferenceAllele(mafRecord.getReferenceAllele());
        mutationEvent.setTumorSeqAllele(mafRecord.getTumorSeqAllele1());
        mutationEvent.setProteinChange(mafRecord.getHgvspShort());
        mutationEvent.setMutationType(mafRecord.getVariantClassification());
        mutationEvent.setFunctionalImpactScore(mafRecord.getMaFimpact());
        mutationEvent.setFisValue(Float.valueOf(mafRecord.getMaFis()));
        mutationEvent.setLinkXvar(mafRecord.getMaLinkVar());
        mutationEvent.setLinkPdb(mafRecord.getMaLinkPdb());
        mutationEvent.setLinkMsa(mafRecord.getMaLinkMsa());
        mutationEvent.setNcbiBuild(mafRecord.getNcbiBuild());
        mutationEvent.setStrand(mafRecord.getStrand());
        mutationEvent.setVariantType(mafRecord.getVariantType());
        mutationEvent.setDbSnpRs(mafRecord.getDbsnpRs());
        mutationEvent.setDbSnpValStatus(mafRecord.getDbsnpValStatus());
        mutationEvent.setOncotatorDbsnpRs(mafRecord.getOncotatorDbsnpRs());
        mutationEvent.setOncotatorRefseqMrnaId(mafRecord.getOncotatorRefseqMrnaId());
        mutationEvent.setOncotatorCodonChange(mafRecord.getOncotatorCodonChange());
        mutationEvent.setOncotatorUniprotEntryName(mafRecord.getOncotatorUniprotEntryName());
        mutationEvent.setOncotatorUniprotAccession(mafRecord.getOncotatorUniprotAccession());
        mutationEvent.setOncotatorProteinPosStart(Integer.valueOf(mafRecord.getOncotatorProteinPosStart()));
        mutationEvent.setOncotatorProteinPosEnd(Integer.valueOf(mafRecord.getOncotatorProteinPosEnd()));
        mutationEvent.setCanonicalTranscript(MutationDataUtils.CANONICAL_TRANSCRIPT);
        
        return mutationEvent;
    }
    
    /**
     * Process the maf record data.
     * 
     * @param mafRecord
     * @return MafRecord
     */
    private MafRecord processMafRecordData(MafRecord mafRecord) {
        // resolve reference and tumor seq allele 
        String referenceAllele = DataFileUtils.isNullOrEmptyValue(mafRecord.getReferenceAllele())?"-":mafRecord.getReferenceAllele();
        String tumorSeqAllele = mafRecord.getTumorSeqAllele1();
        if (!DataFileUtils.isNullOrEmptyValue(mafRecord.getTumorSeqAllele2())) {
            tumorSeqAllele = MutationDataUtils.resolveTumorSeqAllele(referenceAllele, mafRecord.getTumorSeqAllele1(), mafRecord.getTumorSeqAllele2());
        }
        
        // resolve start and stop positions        
        Long startPosition = (Long.valueOf(mafRecord.getStartPosition()) < 0)?0:Long.valueOf(mafRecord.getStartPosition());
        Long endPosition = MutationDataUtils.calculateEndPosition(referenceAllele, tumorSeqAllele, startPosition);
        
        // resolve functional impact score if applicable - if column not in maf then spring automatically makes value null
        String functionalImpactScore = !DataFileUtils.isNullOrEmptyValue(mafRecord.getMaFimpact())?MutationDataUtils.transformOmaScore(mafRecord.getMaFimpact()):null;
        String linkXVar = (!Strings.isNullOrEmpty(mafRecord.getMaLinkVar()))?mafRecord.getMaLinkVar().replace("\"", ""):"";
        String fisValue = !DataFileUtils.isNullOrEmptyValue(mafRecord.getMaFis())?mafRecord.getMaFis():String.valueOf(Float.MIN_VALUE);
        
        // resolve the protein change, protein start position, and protein end position        
        String proteinChange = MutationDataUtils.resolveProteinChange(mafRecord.getHgvspShort(), mafRecord.getAminoAcidChange(), mafRecord.getMaProteinChange());
        Integer proteinStartPosition = 0;
        Integer proteinEndPosition = 0;
        if (!DataFileUtils.isNullOrEmptyValue(mafRecord.getProteinPosition())) {
            proteinStartPosition = MutationDataUtils.resolveProteinStartPosition(mafRecord.getProteinPosition(), proteinChange);
            proteinEndPosition = MutationDataUtils.resolveProteinEndPosition(mafRecord.getProteinPosition(), proteinChange);
        }
        String uniProtAccession = uniProtIdMappingJdbcDaoImpl.mapUniProtIdToAccession(mafRecord.getSwissprot());
        
        // resolve the tumor alt/ref counts and normal alt/ref counts
        Integer tAltCount = MutationDataUtils.resolveTumorAltCount(mafRecord);
        Integer tRefCount = MutationDataUtils.resolveTumorRefCount(mafRecord);
        Integer nAltCount = MutationDataUtils.resolveNormalAltCount(mafRecord);
        Integer nRefCount = MutationDataUtils.resolveNormalRefCount(mafRecord);
        
        // resolve sequence source value
        String sequenceSource = DataFileUtils.isNullOrEmptyValue(mafRecord.getSequenceSource())?"NA":mafRecord.getSequenceSource();
        
        // update maf record with resolved data
        mafRecord.setReferenceAllele(referenceAllele);
        mafRecord.setTumorSeqAllele1(tumorSeqAllele);
        mafRecord.setStartPosition(String.valueOf(startPosition));
        mafRecord.setEndPosition(String.valueOf(endPosition));
        mafRecord.setMaFimpact(functionalImpactScore);
        mafRecord.setMaLinkVar(linkXVar);
        mafRecord.setMaFis(fisValue);
        mafRecord.setHgvspShort(proteinChange);
        mafRecord.setOncotatorProteinPosStart(String.valueOf(proteinStartPosition));
        mafRecord.setOncotatorProteinPosEnd(String.valueOf(proteinEndPosition));
        mafRecord.setOncotatorUniprotAccession(uniProtAccession);
        mafRecord.settAltCount(String.valueOf(tAltCount));
        mafRecord.settRefCount(String.valueOf(tRefCount));
        mafRecord.setnAltCount(String.valueOf(nAltCount));
        mafRecord.setnRefCount(String.valueOf(nRefCount));
        mafRecord.setSequenceSource(sequenceSource);
        
        return mafRecord;
    }
    
    /**
     * Performs basic data screening to determine whether MAF record is acceptable or not.
     * Returns null if MAF record does not pass screening
     * 
     * @param mafRecord
     * @return MafRecord
     */
    private MafRecord screenMafRecord(MafRecord mafRecord) {
        // check validation status value
        if (mafRecord.getValidationStatus() == null || mafRecord.getValidationStatus().equalsIgnoreCase("Wildtype")) {
            LOG.warn("Skipping entry with Validation_Status: Wildtype");
            return null;
        }

        // check mutation type value
        String mutationType = MutationDataUtils.resolveMutationType(mafRecord.getVariantClassification(), mafRecord.getOncotatorVariantClassification());
        if (!DataFileUtils.isNullOrEmptyValue(mutationType) && mutationType.equalsIgnoreCase("rna")) {
            LOG.warn("Skipping entry with mutation type: RNA");
            return null;
        }
        
        // check gene symbol and entrez gene id values
        String hugoGeneSymbol = mafRecord.getHugoSymbol();
        Integer entrezGeneId = !DataFileUtils.isNullOrEmptyValue(mafRecord.getEntrezGeneId())?
                Integer.valueOf(mafRecord.getEntrezGeneId()):null;
        String normalizedChromosome = geneDataUtils.getNormalizedChromosome(mafRecord.getChromosome());        
        if ((DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol) || hugoGeneSymbol.equalsIgnoreCase("unknown")) 
                && (entrezGeneId == null || entrezGeneId <= 0)) {
            LOG.warn("Skipping entry with invalid (Entrez_Gene_Id,Hugo_Symbol,Chromosome): " + 
                        "(" + entrezGeneId + "," + hugoGeneSymbol + "," + normalizedChromosome + ")");
            
            // treat records with unknown gene symbols and invalid entrez gene ids with valid mutation types as igr
            if (hugoGeneSymbol.equalsIgnoreCase("unknown") && MutationDataUtils.isAcceptableMutation(mutationType)) {
                LOG.warn("Treating mutation with gene symbol 'Unknown' as intergenic instead of " + 
                        mutationType);
                mutationType = "IGR";
            }
            else {
                // let mutation type IGR pass so that it can be counted by mutation filter correctly
                return null;
            }
        }
        
        // check if gene can be resolved from MAF record first
        Gene gene = geneDataUtils.resolveGeneFromRecordData(entrezGeneId, hugoGeneSymbol, normalizedChromosome);
        if (gene == null) {
            if (!hugoGeneSymbol.equalsIgnoreCase("unknown") && !mutationType.equals("IGR")) {
                LOG.warn("Could not resolve gene from (Entrez_Gene_Id,Hugo_Symbol,Chromosome): " + 
                        "(" + entrezGeneId + "," + hugoGeneSymbol + "," + normalizedChromosome + ")");
                return null;
            }
        }
        else {
            // update entrez gene id, hugo gene symbol, and chromosome (if necessary) with resolved gene data
            entrezGeneId = gene.getEntrezGeneId();
            hugoGeneSymbol = gene.getHugoGeneSymbol();
            if (DataFileUtils.isNullOrEmptyValue(normalizedChromosome)) {
                normalizedChromosome = geneDataUtils.getChromosomeFromCytoband(gene.getCytoband());
            }
            // check normalized chromosome value
            if (Strings.isNullOrEmpty(normalizedChromosome)) {            
                LOG.warn("Skipping entry with chromosome value: " + mafRecord.getChromosome());
                return null;
            }
        }

        // update maf record with any resolved values from data checks above
        mafRecord.setChromosome(normalizedChromosome);
        mafRecord.setVariantClassification(mutationType);
        mafRecord.setEntrezGeneId(String.valueOf(entrezGeneId));
        mafRecord.setHugoSymbol(hugoGeneSymbol);
        
        return mafRecord;
    }
    
}

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
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.model.StructuralVariantRecord;

import com.google.common.base.Strings;
import org.apache.commons.logging.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class StructuralVariantDataProcessor implements ItemProcessor<StructuralVariantRecord, StructuralVariant> {
    
    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(StructuralVariantDataProcessor.class);

    @Override
    public StructuralVariant process(StructuralVariantRecord structuralVariantRecord) throws Exception {
        StructuralVariantRecord screenedRecord = screenStructuralVariantRecord(structuralVariantRecord);
        if (screenedRecord == null) {
            return null;            
        }        
        StructuralVariant structuralVariant = transformStructuralVariantRecord(screenedRecord);
        
        return structuralVariant;
    }
    
    /**
     * Transforms structural variant record to instance of StructuralVariant.
     * 
     * @param structuralVariantRecord
     * @return StructuralVariant
     */
    private StructuralVariant transformStructuralVariantRecord(StructuralVariantRecord structuralVariantRecord) {
        // resolve integer values for structural variant fields
        Integer mapQ = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getMapQ())?null:Integer.valueOf(structuralVariantRecord.getMapQ());
        Integer normalReadCount = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getNormalReadCount())?0:Integer.valueOf(structuralVariantRecord.getNormalReadCount());
        Integer normalVariantCount = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getNormalVariantCount())?null:Integer.valueOf(structuralVariantRecord.getNormalVariantCount());
        Integer svLength = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getSvLength())?0:Integer.valueOf(structuralVariantRecord.getSvLength());
        Integer tumorReadCount = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getTumorReadCount())?null:Integer.valueOf(structuralVariantRecord.getTumorReadCount());
        Integer tumorVariantCount = DataFileUtils.isNullOrEmptyValue(structuralVariantRecord.getTumorVariantCount())?null:Integer.valueOf(structuralVariantRecord.getTumorVariantCount());
        
        // create new StructuralVariant instance with processed/resolved data from above
        StructuralVariant structuralVariant = new StructuralVariant();
        structuralVariant.setGeneticProfileId(structuralVariantRecord.getGeneticProfileId());
        structuralVariant.setSampleId(structuralVariantRecord.getSampleInternalId());
        structuralVariant.setSite1Gene(structuralVariantRecord.getSite1Gene());
        structuralVariant.setSite1Chrom(structuralVariantRecord.getSite1Chrom());
        structuralVariant.setSite2Gene(structuralVariantRecord.getSite2Gene());
        structuralVariant.setSite2Chrom(structuralVariantRecord.getSite2Chrom());
        structuralVariant.setBreakpointType(structuralVariantRecord.getBreakpointType());
        structuralVariant.setAnnotation(structuralVariantRecord.getAnnotation());
        structuralVariant.setComments(structuralVariantRecord.getComments());
        structuralVariant.setConfidenceClass(structuralVariantRecord.getConfidenceClass());
        structuralVariant.setConnectionType(structuralVariantRecord.getConnectionType());
        structuralVariant.setEventInfo(structuralVariantRecord.getEventInfo());
        structuralVariant.setMapQ(mapQ);
        structuralVariant.setNormalReadCount(normalReadCount);
        structuralVariant.setNormalVariantCount(normalVariantCount);
        structuralVariant.setPairedEndReadSupport(structuralVariantRecord.getPairedEndReadSupport());
        structuralVariant.setSite1Desc(structuralVariantRecord.getSite1Desc());
        structuralVariant.setSite1Pos(Integer.valueOf(structuralVariantRecord.getSite1Pos()));
        structuralVariant.setSite2Desc(structuralVariantRecord.getSite2Desc());
        structuralVariant.setSite2Pos(Integer.valueOf(structuralVariantRecord.getSite2Pos()));
        structuralVariant.setSplitReadSupport(structuralVariantRecord.getSplitReadSupport());
        structuralVariant.setSvClassName(structuralVariantRecord.getSvClassName());
        structuralVariant.setSvDesc(structuralVariantRecord.getSvDesc());
        structuralVariant.setSvLength(svLength);
        structuralVariant.setTumorReadCount(tumorReadCount);
        structuralVariant.setTumorVariantCount(tumorVariantCount);
        
        return structuralVariant;
    }
    
    /**
     * Performs basic data screening to determine whether structural variant record is acceptable or not. 
     * Returns null if structural variant record does not pass screening
     * 
     * @param structuralVariantRecord
     * @return StructuralVariantRecord
     */
    private StructuralVariantRecord screenStructuralVariantRecord(StructuralVariantRecord structuralVariantRecord) {
        // check if genes can be resolved from structural variant record
        String geneSymbol1 = structuralVariantRecord.getSite1Gene();
        String normChrom1 = geneDataUtils.getNormalizedChromosome(structuralVariantRecord.getSite1Chrom());
        if (DataFileUtils.isNullOrEmptyValue(geneSymbol1)) {
            LOG.warn("Skipping entry with invalid Site1_Gene: " + geneSymbol1);
            return null;
        }
        Gene gene1 = geneDataUtils.resolveGeneFromRecordData(geneSymbol1, normChrom1);
        if (gene1 == null) {
            LOG.warn("Could not resolve gene from (Site1_Gene,Site1_Chrom): (" + geneSymbol1 + "," + normChrom1 + ")");
            return null;
        }
        // update normalized chromosome value if null or empty
        if (Strings.isNullOrEmpty(normChrom1)) {
            normChrom1 = gene1.getChromosome();
        }
        
        String geneSymbol2 = structuralVariantRecord.getSite2Gene();
        String normChrom2 = geneDataUtils.getNormalizedChromosome(structuralVariantRecord.getSite2Chrom());
        if (DataFileUtils.isNullOrEmptyValue(geneSymbol2)) {
            LOG.warn("Skipping entry with invalid Site2_Gene: " + geneSymbol2);
            return null;
        }
        Gene gene2 = geneDataUtils.resolveGeneFromRecordData(geneSymbol1, normChrom1);
        if (gene2 == null) {
            LOG.warn("Could not resolve gene from (Site2_Gene,Site2_Chrom): (" + geneSymbol2 + "," + normChrom2 + ")");
            return null;
        }
        // update normalized chromosome value if null or empty
        if (Strings.isNullOrEmpty(normChrom2)) {
            normChrom2 = gene2.getChromosome();
        }
        // update gene and chromosome values for structural variant record
        structuralVariantRecord.setSite1Gene(geneSymbol1);
        structuralVariantRecord.setSite1Chrom(normChrom1);
        structuralVariantRecord.setSite2Gene(geneSymbol2);
        structuralVariantRecord.setSite2Chrom(normChrom2);
        
        return structuralVariantRecord;
    }
    
}

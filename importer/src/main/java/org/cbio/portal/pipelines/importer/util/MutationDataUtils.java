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

package org.cbio.portal.pipelines.importer.util;

import org.mskcc.cbio.model.Mutation;
import org.cbio.portal.pipelines.importer.model.MafRecord;

import java.util.*;
import java.util.regex.*;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Utils class for resolving mutation data
 * 
 * @author ochoaa
 */
@Repository
public class MutationDataUtils {
    
    public static final boolean CANONICAL_TRANSCRIPT = true;

    private static final String[] ignoredMutationTypes = {"silent", "loh", "wildtype", 
            "3'utr", "5'utr", "5'flank", "igr", "rna"};

    private static final HashMap<String, String> transformedOmaScoreMap = new HashMap<>();
    @Autowired
    private void setTransformedOmaScoreMap() {
        transformedOmaScoreMap.put("H", "H");
        transformedOmaScoreMap.put("high", "H");
        transformedOmaScoreMap.put("M", "M");
        transformedOmaScoreMap.put("medium", "M");
        transformedOmaScoreMap.put("L", "L");
        transformedOmaScoreMap.put("low", "L");
        transformedOmaScoreMap.put("N", "N");
        transformedOmaScoreMap.put("neutral", "N");
        transformedOmaScoreMap.put("[sent]", DataFileUtils.DEFAULT_NOT_AVAILABLE);
    }
    
    /**
     * Calculate the end position from the reference allele, tumor seq allele, and start position.
     * 
     * @param referenceAllele
     * @param tumorSeqAllele
     * @param startPosition
     * @return Long
     */
    public static Long calculateEndPosition(String referenceAllele, String tumorSeqAllele, Long startPosition) {
        long endPosition = 0;
        
        if (referenceAllele.equals("-")) {
            endPosition = startPosition + 1;            
        }
        else {
            endPosition = startPosition + tumorSeqAllele.length() - 1;
        }
        
        return endPosition;
    }
    
    /**
     * Resolve the tumor seq allele given a reference allele, tumor seq allele 1, and tumor seq allele 2.
     * 
     * @param referenceAllele
     * @param tumorSeqAllele1
     * @param tumorSeqAllele2
     * @return String
     */
    public static String resolveTumorSeqAllele(String referenceAllele, String tumorSeqAllele1, String tumorSeqAllele2) {        
        String tumorSeqAllele = tumorSeqAllele1;
        if (!Strings.isNullOrEmpty(referenceAllele) && referenceAllele.equals(tumorSeqAllele1)) {
            tumorSeqAllele = tumorSeqAllele2;
        }
        
        return tumorSeqAllele;
    }
    
    /**
     * Determine if given mutation type is acceptable or not.
     * 
     * @param mutationType
     * @return boolean
     */
    public static boolean isAcceptableMutation(String mutationType) {        
        boolean valid = true;
        if (DataFileUtils.isNullOrEmptyValue(mutationType)) {
            valid = false;
        }
        else {
            for (String mutType : ignoredMutationTypes) {
                if (mutType.equals("rna") && mutationType.equalsIgnoreCase(mutType)) {
                    valid = false;
                }
                else if (mutationType.toLowerCase().startsWith(mutType)) {
                    valid = false;
                }
            }            
        }
        
        return valid;
    }
    
    /**
     * Resolve mutation type given a variant classification and oncotator variant classification.
     * 
     * @param variantClassification
     * @param oncotatorVariantClassification
     * @return String
     */
    public static String resolveMutationType(String variantClassification, String oncotatorVariantClassification) {
        String mutationType = variantClassification;
        if (!isAcceptableMutation(variantClassification) && 
                isAcceptableMutation(oncotatorVariantClassification)) {
            mutationType = oncotatorVariantClassification;
        }
        
        return mutationType;
    }
    
    /**
     * Resolve protein change value given a protein change and amino acid change values.
     * 
     * @param proteinChange
     * @param aminoAcidChange
     * @return String
     */
    public static String resolveProteinChange(String proteinChange, String aminoAcidChange, String maProteinChange) {
        String resolvedProteinChange = "MUTATED";
        if (!DataFileUtils.isNullOrEmptyValue(proteinChange)) {
            resolvedProteinChange = proteinChange;
        }
        else if (!DataFileUtils.isNullOrEmptyValue(aminoAcidChange)) {
            resolvedProteinChange = aminoAcidChange;
        }
        else if (!DataFileUtils.isNullOrEmptyValue(maProteinChange)) {
            resolvedProteinChange = maProteinChange;
        }
        
        return getNormalizedProteinChange(resolvedProteinChange);
    }
    
    /**
     * Removes the starting 'p.' from a given protein change if found.
     * 
     * @param proteinChange
     * @return String
     */
    public static String getNormalizedProteinChange(String proteinChange) {
        if (proteinChange.startsWith("p.")) {
            proteinChange = proteinChange.substring(2);
        }
        return proteinChange;
    }
    
    /**
     * Resolve the protein start position from protein position and protein change.
     * 
     * @param proteinPosition
     * @param proteinChange
     * @return Integer
     */
    public static Integer resolveProteinStartPosition(String proteinPosition, String proteinChange) {        
        // parts[0] is the protein start-end positions, parts[1] is the length
        String[] positions = proteinPosition.split("/")[0].split("-");

        Integer startPosition = DataFileUtils.DEFAULT_MISSING_INT;
        try {
            startPosition = Integer.valueOf(positions[0]);
        }
        catch (ArrayIndexOutOfBoundsException | NumberFormatException ex) {}
        
        // if start position is -1 then try extracting from protein change
        if (startPosition.equals(DataFileUtils.DEFAULT_MISSING_INT)) {
            Pattern p = Pattern.compile(".*[A-Z]([0-9]+)[^0-9]+");
            Matcher m = p.matcher(proteinChange);
            if (m.find()) {
                startPosition = Integer.valueOf(m.group(1));
            }
        }        


        return startPosition;
    }
    
    /**
     * Resolve the protein end position from protein position.
     * If null or missing int then return start position
     * 
     * @param proteinPosition
     * @param proteinChange
     * @return Integer
     */
    public static Integer resolveProteinEndPosition(String proteinPosition, String proteinChange) {
        // parts[0] is the protein start-end positions, parts[1] is the length
        String[] positions = proteinPosition.split("/")[0].split("-");
        
        Integer endPosition = DataFileUtils.DEFAULT_MISSING_INT;
        try {
            endPosition = Integer.valueOf(positions[1]);
        }
        catch (ArrayIndexOutOfBoundsException | NumberFormatException ex) {}        
        
        // change value of end position if -1
        if (endPosition.equals(DataFileUtils.DEFAULT_MISSING_INT)) {
            return resolveProteinStartPosition(proteinPosition, proteinChange);
        }
        
        return endPosition;
    }
    
    /**
     * Resolve the tumor alt count from a given MAF record.
     * Returns default missing int value if missing from record
     * 
     * @param mafRecord
     * @return Integer
     */
    public static Integer resolveTumorAltCount(MafRecord mafRecord) {
        Integer result = DataFileUtils.DEFAULT_MISSING_INT;
        
        try {
            if (!DataFileUtils.isNullOrMissingInt(mafRecord.gettAltCount())) {
                result = Integer.valueOf(mafRecord.gettAltCount());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.gettVarCov())) {
                result = Integer.valueOf(mafRecord.gettVarCov());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getTumorDepth()) && 
                    !DataFileUtils.isNullOrMissingInt(mafRecord.getTumorVaf())) {
                result = Math.round(Integer.valueOf(mafRecord.getTumorDepth()) * Integer.valueOf(mafRecord.getTumorVaf()));
            }
        }
        catch (NumberFormatException ex) {}

        return result;
    }
    
    /**
     * Resolve the tumor ref count from a given MAF record.
     * Returns default missing int value if missing from record
     * 
     * @param mafRecord
     * @return Integer
     */
    public static Integer resolveTumorRefCount(MafRecord mafRecord) {
        Integer result = DataFileUtils.DEFAULT_MISSING_INT;
        
        try {
            if (!DataFileUtils.isNullOrMissingInt(mafRecord.gettRefCount())) {
                result = Integer.valueOf(mafRecord.gettRefCount());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.gettVarCov())) {
                result = Integer.valueOf(mafRecord.gettTotCov()) - Integer.valueOf(mafRecord.gettVarCov());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getTumorDepth()) &&
                    !DataFileUtils.isNullOrMissingInt(mafRecord.getTumorVaf())) {
                result = Integer.valueOf(mafRecord.getTumorDepth()) - Math.round(Integer.valueOf(mafRecord.getTumorDepth()) * Integer.valueOf(mafRecord.getTumorVaf()));
            }
        }
        catch (NumberFormatException ex) {}

        return result;
    }
    
    /**
     * Resolve the normal alt count from a given MAF record.
     * Returns default missing int value if missing from record
     * 
     * @param mafRecord
     * @return Integer
     */
    public static Integer resolveNormalAltCount(MafRecord mafRecord) {
        Integer result = DataFileUtils.DEFAULT_MISSING_INT;
        
        try {
            if (!DataFileUtils.isNullOrMissingInt(mafRecord.getnAltCount())) {
                result = Integer.valueOf(mafRecord.getnAltCount());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getnVarCov())) {
                result = Integer.valueOf(mafRecord.getnVarCov());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getNormalDepth()) &&
                    !DataFileUtils.isNullOrMissingInt(mafRecord.getNormalVaf())) {
                result = Math.round(Integer.valueOf(mafRecord.getNormalDepth()) * Integer.valueOf(mafRecord.getNormalVaf()));
            }
        }
        catch (NumberFormatException ex) {}

        return result;
    }
    
    /**
     * Resolve the normal ref count from a given MAF record.
     * Returns default missing int value if missing from record
     * 
     * @param mafRecord
     * @return Integer
     */
    public static Integer resolveNormalRefCount(MafRecord mafRecord) {
        Integer result = DataFileUtils.DEFAULT_MISSING_INT;
        
        try {
            if (!DataFileUtils.isNullOrMissingInt(mafRecord.getnRefCount())) {
                result = Integer.valueOf(mafRecord.getnRefCount());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getnVarCov()) && 
                    !DataFileUtils.isNullOrMissingInt(mafRecord.getnTotCov())) {
                result = Integer.valueOf(mafRecord.getnTotCov()) - Integer.valueOf(mafRecord.getnVarCov());
            }
            else if (!DataFileUtils.isNullOrMissingInt(mafRecord.getNormalDepth()) &&
                    !DataFileUtils.isNullOrMissingInt(mafRecord.getNormalVaf())) {
                result = Integer.valueOf(mafRecord.getNormalDepth()) - Math.round(Integer.valueOf(mafRecord.getNormalDepth()) * Integer.valueOf(mafRecord.getNormalVaf()));
            }
        }
        catch (NumberFormatException ex) {}

        return result;
    }
    
    /**
     * Transform the OMA score.
     * 
     * @param omaScore
     * @return String
     */
    public static String transformOmaScore(String omaScore) {
        if (transformedOmaScoreMap.containsKey(omaScore.toUpperCase())) {
            return transformedOmaScoreMap.get(omaScore.toUpperCase());
        }
        else if (transformedOmaScoreMap.containsKey(omaScore.toLowerCase())) {
            return transformedOmaScoreMap.get(omaScore.toLowerCase());
        }
        else {
            return omaScore;
        }
    }
    
    /**
     * Merge given mutations. 
     * 
     * @param mut1
     * @param mut2
     * @return Mutation
     */
    public static Mutation mergeMutationData(Mutation mut1, Mutation mut2) {
        Mutation mergedMutation = mut1;
        if (!mut1.getMatchedNormSampleBarcode().equalsIgnoreCase(mut2.getMatchedNormSampleBarcode()) &&
                DataFileUtils.isNormalSample(mut2.getMatchedNormSampleBarcode())) {
            mergedMutation = mut2;
        }
        else if (!mut1.getValidationStatus().equalsIgnoreCase(mut2.getValidationStatus()) && 
                (mut2.getValidationStatus().equalsIgnoreCase("Valid") || mut2.getValidationStatus().equalsIgnoreCase("VALIDATED"))) {
            mergedMutation = mut2;
        }
        else if (!mut1.getMutationStatus().equalsIgnoreCase(mut2.getMutationStatus())) {           
                if (mut2.getMutationStatus().equalsIgnoreCase("Germline")) {
                    mergedMutation = mut2;
                }
                else if (mut2.getMutationStatus().equalsIgnoreCase("SOMATIC") && 
                        !mut1.getMutationStatus().equalsIgnoreCase("Germline")) {
                    mergedMutation = mut2;
                }
        }        
        // merge centers for mutations
        Set<String> mut1Centers = new HashSet<>(Arrays.asList(mut1.getCenter().split(";")));
        Set<String> mut2Centers = new HashSet<>(Arrays.asList(mut2.getCenter().split(";")));
        Set<String> mergedCenters = Sets.union(mut1Centers, mut2Centers);
        mergedCenters.remove("NA");
        mergedMutation.setCenter(StringUtils.join(mergedCenters, ";"));
        mergedMutation.setMutationEventId(mut1.getMutationEventId());
        
        return mergedMutation;
    }
    
}

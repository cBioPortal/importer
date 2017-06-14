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

import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutationFilter {
    
    // params for filtered mutations counts
    private Integer decisions;
    private Integer accepts;
    private Integer mutationStatusNoneRejects;
    private Integer silentOrIntronRejects;
    private Integer lohOrWildtypeRejects;
    private Integer redactedRejects;
    private Integer utrRejects;
    private Integer igrRejects;    
    
    public static final Set<Integer> whiteListGenesForPromoterMutations = new HashSet<>();
    @Autowired
    private void setWhiteListGeneEntrezIds() {
        whiteListGenesForPromoterMutations.add(7015);
    }    
    
    public MutationFilter() {
        this.decisions = 0;
        this.accepts = 0;
        this.mutationStatusNoneRejects = 0;
        this.silentOrIntronRejects = 0;
        this.lohOrWildtypeRejects = 0;
        this.redactedRejects = 0;
        this.utrRejects = 0;
        this.igrRejects = 0;        
    }

    /**
     * update decisions count
     */
    public void updateDecisions() {
        this.decisions++;
    }
    
    /**
     * update accepts count
     */
    public void updateAccepts() {
        this.accepts++;
    }
    
    /** 
     * update mutation status none rejects count
     */
    public void updateMutationStatusNoneRejects() {
        this.mutationStatusNoneRejects++;
    }
    
    /** 
     * update silent or intron rejects count
     */
    public void updateSilentOrIntronRejects() {
        this.silentOrIntronRejects++;
    }
    
    /** 
     * update loh or wildtype rejects count
     */
    public void updateLohOrWildtypeRejects() {
        this.lohOrWildtypeRejects++;
    }

    /** 
     * update redacted rejects count
     */
    public void updateRedactedRejects() {
        this.redactedRejects++;
    }

    /** 
     * update utr rejects count
     */
    public void updateUtrRejects() {
        this.utrRejects++;
    }

    /** 
     * update igr rejects count 
     */
    public void updateIgrRejects() {
        this.igrRejects++;
    }    
    
    /**
     * Determines whether mutation should be filtered out or not.
     * 
     * @param mutation
     * @return boolean
     */
    public boolean acceptMutation(Mutation mutation) {
        decisions++;
        
        // reject records with Mutation_Status 'None'
        if (isPatternMatch(mutation.getMutationStatus(), new String[]{"None"})) {
            mutationStatusNoneRejects++;
            return false;
        }
        // reject silent and intronic mutations
        if (isPatternMatch(mutation.getMutationEvent().getMutationType(), new String[]{"Silent", "Intron"})) {
            silentOrIntronRejects++;
            return false;
        }
        // reject loh and wildtype mutations
        if (isPatternMatch(mutation.getMutationStatus(), new String[]{"LOH", "Wildtype"})) {
            lohOrWildtypeRejects++;
            return false;
        }
        // reject redacted muations
        if (isPatternMatch(mutation.getValidationStatus(), new String[]{"Redacted"})) {
            redactedRejects++;
            return false;
        }
        // reject 3'utr and 5'utr mutations
        if (isPatternMatch(mutation.getMutationEvent().getMutationType(), new String[]{"3'UTR", "3'Flank", "5'UTR"})) {
            utrRejects++;
            return false;
        }
        // accept 5'Flank promoter mutations for white listed genes
        if (isPatternMatch(mutation.getMutationEvent().getMutationType(), new String[]{"5'Flank"})) {
            if (whiteListGenesForPromoterMutations.contains(mutation.getEntrezGeneId())) {
                mutation.getMutationEvent().setMutationType("Promoter");
            }
            else {
                utrRejects++;
                return false;
            }
        }
        // mutations with hugo symbols that are unknown will be treated as intergenic
        if (isPatternMatch(mutation.getMutationEvent().getMutationType(), new String[]{"IGR"})) {
            igrRejects++;
            return false;
        }
        
        // increment accepted mutations
        accepts++;
        
        return true;
    }    

    /**
     * Checks if value starts with given pattern.
     * 
     * @param value
     * @param patterns
     * @return boolean
     */
    private boolean isPatternMatch(String value, String[] patterns) {        
        if (DataFileUtils.isNullOrEmptyValue(value)) {
            return true;
        }
        
        boolean hasPattern = false;
        for (String pattern : patterns) {
            if (value.toLowerCase().startsWith(pattern.toLowerCase())) {
                hasPattern = true;
            }
        }        
        
        return hasPattern;
    }

    /**
     * Prints the MutationFilter summary statistics. 
     */
    public void printSummaryStatistics(){
        String summary = "Mutation filter decisions: " + decisions +
                "\nRejects: " + (decisions-accepts) +
                "\nMutation Status 'None' Rejects:  " + mutationStatusNoneRejects +
                "\nSilent or Intron Rejects:  " + silentOrIntronRejects +
                "\nUTR Rejects:  " + utrRejects +
                "\nIGR Rejects:  " + igrRejects +
                "\nLOH or Wild Type Rejects:  " + lohOrWildtypeRejects;
        System.out.println(summary);
    }    
    
}

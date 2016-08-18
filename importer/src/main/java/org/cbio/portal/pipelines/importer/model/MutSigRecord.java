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

package org.cbio.portal.pipelines.importer.model;

import java.util.*;

/**
 *
 * @author ochoaa
 */
public class MutSigRecord {
    
    private Integer cancerStudyId;
    private String rank;
    private String hugoSymbol;
    private String numBasesCovered;
    private String numMutations;
    private String pValue;
    private String qValue;

    /**
     * @return the cancerStudyId
     */
    public Integer getCancerStudyId() {
        return cancerStudyId;
    }

    /**
     * @param cancerStudyId the cancerStudyId to set
     */
    public void setCancerStudyId(Integer cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }
    
    /**
     * @return the rank
     */
    public String getRank() {
        return rank;
    }

    /**
     * @param rank the rank to set
     */
    public void setRank(String rank) {
        this.rank = rank;
    }

    /**
     * @return the hugoSymbol
     */
    public String getHugoSymbol() {
        return hugoSymbol;
    }

    /**
     * @param hugoSymbol the hugoSymbol to set
     */
    public void setHugoSymbol(String hugoSymbol) {
        this.hugoSymbol = hugoSymbol;
    }

    /**
     * @return the numBasesCovered
     */
    public String getNumBasesCovered() {
        return numBasesCovered;
    }

    /**
     * @param numBasesCovered the numBasesCovered to set
     */
    public void setNumBasesCovered(String numBasesCovered) {
        this.numBasesCovered = numBasesCovered;
    }

    /**
     * @return the numMutations
     */
    public String getNumMutations() {
        return numMutations;
    }

    /**
     * @param numMutations the numMutations to set
     */
    public void setNumMutations(String numMutations) {
        this.numMutations = numMutations;
    }

    /**
     * @return the pValue
     */
    public String getpValue() {
        return pValue;
    }

    /**
     * @param pValue the pValue to set
     */
    public void setpValue(String pValue) {
        this.pValue = pValue;
    }

    /**
     * @return the qValue
     */
    public String getqValue() {
        return qValue;
    }

    /**
     * @param qValue the qValue to set
     */
    public void setqValue(String qValue) {
        this.qValue = qValue;
    }
    
    /**
     * @return the mutsig staging data map (column -> field)
     */
    public Map<String, String> getMutSigStagingDataMap() {
        Map<String, String> map = new HashMap<>();
        map.put("rank", "rank");
        map.put("gene", "hugoSymbol");
        map.put("p", "pValue");
        map.put("q", "qValue");
        
        // for mutsig columns that have multiple possible column names
        map.put("N", "numBasesCovered");
        map.put("Nnon", "numBasesCovered");
        map.put("n", "numMutations");
        map.put("nnon", "numMutations");

        return map;
    }
    
}

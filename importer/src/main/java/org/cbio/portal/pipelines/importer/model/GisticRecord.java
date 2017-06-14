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
public class GisticRecord {
    
    private Integer cancerStudyId;
    private String chromosome;
    private String peakStart;
    private String peakEnd;
    private String genesInRegion;
    private String qValue;
    private String cytoband;
    private String amp;

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
     * @return the chromosome
     */
    public String getChromosome() {
        return chromosome;
    }

    /**
     * @param chromosome the chromosome to set
     */
    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    /**
     * @return the peakStart
     */
    public String getPeakStart() {
        return peakStart;
    }

    /**
     * @param peakStart the peakStart to set
     */
    public void setPeakStart(String peakStart) {
        this.peakStart = peakStart;
    }

    /**
     * @return the peakEnd
     */
    public String getPeakEnd() {
        return peakEnd;
    }

    /**
     * @param peakEnd the peakEnd to set
     */
    public void setPeakEnd(String peakEnd) {
        this.peakEnd = peakEnd;
    }

    /**
     * @return the genesInRegion
     */
    public String getGenesInRegion() {
        return genesInRegion;
    }

    /**
     * @param genesInRegion the genesInRegion to set
     */
    public void setGenesInRegion(String genesInRegion) {
        this.genesInRegion = genesInRegion;
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
     * @return the cytoband
     */
    public String getCytoband() {
        return cytoband;
    }

    /**
     * @param cytoband the cytoband to set
     */
    public void setCytoband(String cytoband) {
        this.cytoband = cytoband;
    }

    /**
     * @return the amp
     */
    public String getAmp() {
        return amp;
    }

    /**
     * @param amp the amp to set
     */
    public void setAmp(String amp) {
        this.amp = amp;
    }
    
    /**
     * @return the gistic staging data map (column -> field)
     */
    public Map<String, String> getGisticStagingDataMap() {
        Map<String, String> map = new HashMap<>();
        map.put("chromosome", "chromosome");
        map.put("peak_start", "peakStart");
        map.put("peak_end", "peakEnd");
        map.put("genes_in_region", "genesInRegion");
        map.put("q_value", "qValue");
        map.put("cytoband", "cytoband");
        map.put("amp", "amp");
        
        return map;
    }
    
}

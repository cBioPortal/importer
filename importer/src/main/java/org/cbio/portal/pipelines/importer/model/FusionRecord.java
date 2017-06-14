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
public class FusionRecord {
    
    private Integer sampleId;
    private Integer geneticProfileId;
    private String hugoSymbol;
    private String entrezGeneId;
    private String center;
    private String tumorSampleBarcode;
    private String fusion;
    private String dnaSupport;
    private String rnaSupport;
    private String method;
    private String frame;
    private String comments;

    /**
     * @return the sampleId
     */
    public Integer getSampleId() {
        return sampleId;
    }

    /**
     * @param sampleId the sampleId to set
     */
    public void setSampleId(Integer sampleId) {
        this.sampleId = sampleId;
    }
    
    /**
     * @return the geneticProfileId
     */
    public Integer getGeneticProfileId() {
        return geneticProfileId;
    }

    /**
     * @param geneticProfileId the geneticProfileId to set
     */
    public void setGeneticProfileId(Integer geneticProfileId) {
        this.geneticProfileId = geneticProfileId;
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
     * @return the entrezGeneId
     */
    public String getEntrezGeneId() {
        return entrezGeneId;
    }

    /**
     * @param entrezGeneId the entrezGeneId to set
     */
    public void setEntrezGeneId(String entrezGeneId) {
        this.entrezGeneId = entrezGeneId;
    }

    /**
     * @return the center
     */
    public String getCenter() {
        return center;
    }

    /**
     * @param center the center to set
     */
    public void setCenter(String center) {
        this.center = center;
    }

    /**
     * @return the tumorSampleBarcode
     */
    public String getTumorSampleBarcode() {
        return tumorSampleBarcode;
    }

    /**
     * @param tumorSampleBarcode the tumorSampleBarcode to set
     */
    public void setTumorSampleBarcode(String tumorSampleBarcode) {
        this.tumorSampleBarcode = tumorSampleBarcode;
    }

    /**
     * @return the fusion
     */
    public String getFusion() {
        return fusion;
    }

    /**
     * @param fusion the fusion to set
     */
    public void setFusion(String fusion) {
        this.fusion = fusion;
    }

    /**
     * @return the dnaSupport
     */
    public String getDnaSupport() {
        return dnaSupport;
    }

    /**
     * @param dnaSupport the dnaSupport to set
     */
    public void setDnaSupport(String dnaSupport) {
        this.dnaSupport = dnaSupport;
    }

    /**
     * @return the rnaSupport
     */
    public String getRnaSupport() {
        return rnaSupport;
    }

    /**
     * @param rnaSupport the rnaSupport to set
     */
    public void setRnaSupport(String rnaSupport) {
        this.rnaSupport = rnaSupport;
    }

    /**
     * @return the method
     */
    public String getMethod() {
        return method;
    }

    /**
     * @param method the method to set
     */
    public void setMethod(String method) {
        this.method = method;
    }

    /**
     * @return the frame
     */
    public String getFrame() {
        return frame;
    }

    /**
     * @param frame the frame to set
     */
    public void setFrame(String frame) {
        this.frame = frame;
    }

    /**
     * @return the comments
     */
    public String getComments() {
        return comments;
    }

    /**
     * @param comments the comments to set
     */
    public void setComments(String comments) {
        this.comments = comments;
    }
    
    /**
     * @return the Fusion staging data map (column -> field)
     */
    public Map<String, String> getFusionStagingData() {
        Map<String, String> map = new HashMap<>();
        map.put("Hugo_Symbol", "hugoSymbol");
        map.put("Entrez_Gene_Id", "entrezGeneId");
        map.put("Center", "center");
        map.put("Tumor_Sample_Barcode", "tumorSampleBarcode");
        map.put("Fusion", "fusion");
        map.put("DNA_support", "dnaSupport");
        map.put("RNA_support", "rnaSupport");
        map.put("Method", "method");
        map.put("Frame", "frame");
        map.put("Comments", "comments");
        
        return map;
    }
    
}

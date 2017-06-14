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

import org.mskcc.cbio.model.*;

import java.util.*;

/**
 *
 * @author ochoaa
 */
public class ProfileDataRecord {
    
    private Integer geneticProfileId;
    private String hugoSymbol;
    private String entrezGeneId;
    private String compositeElementRef;
    private String arrayId;
    private List<Gene> compositeGeneList;
    private Map<Integer, String> caseProfileDataMap;
    private List<CnaEvent> cnaEvents;
    private boolean rppaProfile;
    private boolean cnaData;

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
     * @return the compositeElementRef
     */
    public String getCompositeElementRef() {
        return compositeElementRef;
    }

    /**
     * @param compositeElementRef the compositeElementRef to set
     */
    public void setCompositeElementRef(String compositeElementRef) {
        this.compositeElementRef = compositeElementRef;
    }

    /**
     * @return the arrayId
     */
    public String getArrayId() {
        return arrayId;
    }

    /**
     * @param arrayId the arrayId to set
     */
    public void setArrayId(String arrayId) {
        this.arrayId = arrayId;
    }
    
    /**
     * @return the compositeGeneList
     */
    public List<Gene> getCompositeGeneList() {
        return compositeGeneList;
    }

    /**
     * @param compositeGeneList the compositeGeneList to set
     */
    public void setCompositeGeneList(List<Gene> compositeGeneList) {
        this.compositeGeneList = compositeGeneList;
    }

    /**
     * @return the caseProfileDataMap
     */
    public Map<Integer, String> getCaseProfileDataMap() {
        return caseProfileDataMap;
    }

    /**
     * @param caseProfileDataMap the caseProfileDataMap to set
     */
    public void setCaseProfileDataMap(Map<Integer, String> caseProfileDataMap) {
        this.caseProfileDataMap = caseProfileDataMap;
    }

    /**
     * @return the cnaEvents
     */
    public List<CnaEvent> getCnaEvents() {
        return cnaEvents;
    }

    /**
     * @param cnaEvents the cnaEvents to set
     */
    public void setCnaEvents(List<CnaEvent> cnaEvents) {
        this.cnaEvents = cnaEvents;
    }

    /**
     * @return the rppaProfile
     */
    public boolean isRppaProfile() {
        return rppaProfile;
    }

    /**
     * @param rppaProfile the rppaProfile to set
     */
    public void setRppaProfile(boolean rppaProfile) {
        this.rppaProfile = rppaProfile;
    }

    /**
     * @return the cnaData
     */
    public boolean isCnaData() {
        return cnaData;
    }

    /**
     * @param cnaData the cnaData to set
     */
    public void setCnaData(boolean cnaData) {
        this.cnaData = cnaData;
    }

    /**
     * @return the profile data non-case id map (column -> field)
     */
    public Map<String, String> getNonCaseIdsMap() {
        Map<String, String> map = new HashMap<>();        
        map.put("HUGO_SYMBOL", "hugoSymbol");
        map.put("ENTREZ_GENE_ID", "entrezGeneId");
        map.put("COMPOSITE.ELEMENT.REF", "compositeElementRef");
        
        return map;
    }

}

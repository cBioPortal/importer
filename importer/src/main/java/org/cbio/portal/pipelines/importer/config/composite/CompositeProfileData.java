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

package org.cbio.portal.pipelines.importer.config.composite;

import org.mskcc.cbio.model.*;

import java.util.*;

/**
 *
 * @author ochoaa
 */
public class CompositeProfileData {
    
    private GeneticProfile geneticProfile;
    private Gene gene;
    private Map<Integer, String> caseProfileDataRecords;
    private List<CnaEvent> profileCnaEvents;

    /**
     * @return the geneticProfile
     */
    public GeneticProfile getGeneticProfile() {
        return geneticProfile;
    }

    /**
     * @param geneticProfile the geneticProfile to set
     */
    public void setGeneticProfile(GeneticProfile geneticProfile) {
        this.geneticProfile = geneticProfile;
    }

    /**
     * @return the gene
     */
    public Gene getGene() {
        return gene;
    }

    /**
     * @param gene the gene to set
     */
    public void setGene(Gene gene) {
        this.gene = gene;
    }

    /**
     * @return the caseProfileDataRecords
     */
    public Map<Integer, String> getCaseProfileDataRecords() {
        return caseProfileDataRecords;
    }

    /**
     * @param caseProfileDataRecords the caseProfileDataRecords to set
     */
    public void setCaseProfileDataRecords(Map<Integer, String> caseProfileDataRecords) {
        this.caseProfileDataRecords = caseProfileDataRecords;
    }

    /**
     * @return the profileCnaEvents
     */
    public List<CnaEvent> getProfileCnaEvents() {
        return profileCnaEvents;
    }

    /**
     * @param profileCnaEvents the profileCnaEvents to set
     */
    public void setProfileCnaEvents(List<CnaEvent> profileCnaEvents) {
        this.profileCnaEvents = profileCnaEvents;
    }

}

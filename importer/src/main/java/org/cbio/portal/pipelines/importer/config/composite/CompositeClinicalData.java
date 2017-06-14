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
import java.io.Serializable;


/**
 * Composite clinical data object
 * @author ochoaa
 */
public class CompositeClinicalData implements Serializable {
    
    private Patient patient;
    private Sample sample;    
    private Map<String, PatientClinicalData> patientClinicalData;
    private Map<String, SampleClinicalData> sampleClinicalData;
    private Map<ClinicalAttribute, String> compositeClinicalDataMap;

    /**
     * @return the patient
     */
    public Patient getPatient() {
        return patient;
    }

    /**
     * @param patient the patient to set
     */
    public void setPatient(Patient patient) {
        this.patient = patient;
    }

    /**
     * @return the sample
     */
    public Sample getSample() {
        return sample;
    }

    /**
     * @param sample the sample to set
     */
    public void setSample(Sample sample) {
        this.sample = sample;
    }
    
    /**
     * @return the patientClinicalData
     */
    public Map<String, PatientClinicalData> getPatientClinicalData() {
        return patientClinicalData;
    }

    /**
     * @param patientClinicalData the patientClinicalData to set
     */
    public void setPatientClinicalData(Map<String, PatientClinicalData> patientClinicalData) {
        this.patientClinicalData = patientClinicalData;
    }

    /**
     * @return the sampleClinicalData
     */
    public Map<String, SampleClinicalData> getSampleClinicalData() {
        return sampleClinicalData;
    }

    /**
     * @param sampleClinicalData the sampleClinicalData to set
     */
    public void setSampleClinicalData(Map<String, SampleClinicalData> sampleClinicalData) {
        this.sampleClinicalData = sampleClinicalData;
    }

    /**
     * @return the compositeClinicalDataMap
     */
    public Map<ClinicalAttribute, String> getCompositeClinicalDataMap() {
        return compositeClinicalDataMap;
    }

    /**
     * @param compositeClinicalDataMap the compositeClinicalDataMap to set
     */
    public void setCompositeClinicalDataMap(Map<ClinicalAttribute, String> compositeClinicalDataMap) {
        this.compositeClinicalDataMap = compositeClinicalDataMap;
    }
    
    /**
     * Propagate internal id update to patient clinical data.
     * 
     * @param internalId 
     */
    public void updatePatientInternalId(int internalId) {
        this.patient.setInternalId(internalId);
        if (this.sample.getPatientId() == -1) {
            this.sample.setPatientId(internalId);
        }
        if (!this.patientClinicalData.isEmpty()) {
            this.patientClinicalData.keySet().stream().forEach((attrId) -> {
                this.patientClinicalData.get(attrId).setInternalId(internalId);
            });
        }
    }

    /**
     * Propagate internal id update to sample clinical data.
     * 
     * @param internalId 
     */    
    public void updateSampleInternalId(int internalId) {
        this.sample.setInternalId(internalId);
        if (this.patient.getInternalId() == -1) {
            this.patient.setInternalId(this.sample.getPatientId());
        }
        if (!this.sampleClinicalData.isEmpty()) {
            this.sampleClinicalData.keySet().stream().forEach((attrId) -> {
                this.sampleClinicalData.get(attrId).setInternalId(internalId);
            });
        }
    }    

}

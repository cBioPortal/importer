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
import org.cbio.portal.pipelines.importer.config.composite.CompositeClinicalData;

import java.util.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Processor for composite clinical data
 * @author ochoaa
 */
public class ClinicalDataProcessor implements ItemProcessor<CompositeClinicalData, CompositeClinicalData> {
    
    @Autowired
    PatientJdbcDaoImpl patientJdbcDaoImpl;
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    @Override
    public CompositeClinicalData process(CompositeClinicalData composite) throws Exception {
        // update patient and sample internal ids if possible
        Patient existingPatient = patientJdbcDaoImpl.getPatient(composite.getPatient().getStableId(), composite.getPatient().getCancerStudyId());
        if (existingPatient != null) {
            composite.getPatient().setInternalId(existingPatient.getInternalId());
            
            // if patient exists then sample should also exist by patient internal id
            Sample existingSample = sampleJdbcDaoImpl.getSampleByPatient(composite.getSample().getStableId(), composite.getPatient().getInternalId());
            if (existingSample != null) {
                composite.getSample().setInternalId(existingSample.getInternalId());
            }
        }
       
        // sort filtered clinical data into patient and sample clinical data
        Map<String, PatientClinicalData> patientClinicalData = new HashMap<>();
        Map<String, SampleClinicalData> sampleClinicalData = new HashMap<>();
        Map<ClinicalAttribute, String> filteredClinicalData = composite.getCompositeClinicalDataMap();
        for (ClinicalAttribute attr : filteredClinicalData.keySet()) {
            // if attribute is patient attribute then add to patient clinical data list
            // otherwise add to sample clinical data list
            if (attr.getPatientAttribute()) {
                PatientClinicalData newClinicalDatum = new PatientClinicalData();
                newClinicalDatum.setInternalId(composite.getPatient().getInternalId());
                newClinicalDatum.setAttrId(attr.getAttrId());
                newClinicalDatum.setAttrValue(filteredClinicalData.get(attr));
                patientClinicalData.put(attr.getAttrId(), newClinicalDatum);
            }
            else {
                SampleClinicalData newClinicalDatum = new SampleClinicalData();
                newClinicalDatum.setInternalId(composite.getSample().getInternalId());
                newClinicalDatum.setAttrId(attr.getAttrId());
                newClinicalDatum.setAttrValue(filteredClinicalData.get(attr));
                sampleClinicalData.put(attr.getAttrId(), newClinicalDatum);
            }   
        }
        // set patient and sample clinical data for composite object
        composite.setPatientClinicalData(patientClinicalData);
        composite.setSampleClinicalData(sampleClinicalData);
        
        return composite;
    }

}

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

package org.cbio.portal.pipelines.importer.config.writer;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.model.summary.ClinicalDataSummary;
import org.mskcc.cbio.persistence.jdbc.*;
import org.cbio.portal.pipelines.importer.config.composite.CompositeClinicalData;

import java.util.*;
import org.apache.commons.logging.*;
import com.google.common.base.*;
import com.google.common.collect.*;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class ClinicalDataWriter implements ItemStreamWriter<CompositeClinicalData> {
    
    @Autowired
    ClinicalDataJdbcDaoImpl clinicalDataJdbcDaoImpl;
    
    @Autowired
    PatientJdbcDaoImpl patientJdbcDaoImpl;
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    private int patientCount;
    private int sampleCount;   
    private int patientDataCount;
    private int sampleDataCount;
    
    private final Set<Integer> caseIdSet = new LinkedHashSet<>();
    private final Map<Integer, Map<String, String>> patientClinicalDataAdded = new HashMap<>();
    private final Map<Integer, Map<String, String>> sampleClinicalDataAdded = new HashMap<>();
    
    private static final Log LOG = LogFactory.getLog(ClinicalDataWriter.class);

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        LOG.info("Beginning clinical data batch import");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // insert updated record counts and case id set to execution context
        executionContext.put("patientCount", patientCount);
        executionContext.put("sampleCount", sampleCount);
        executionContext.put("patientDataCount", patientDataCount);
        executionContext.put("sampleDataCount", sampleDataCount);
        executionContext.put("caseList", caseIdSet);
    }

    @Override
    public void close() throws ItemStreamException {}
    
    @Override
    public void write(List<? extends CompositeClinicalData> list) throws Exception {
        List<ClinicalDataSummary> patientClinicalData = new ArrayList();
        List<ClinicalDataSummary> sampleClinicalData = new ArrayList();

        // for each composite clinical data, update internal ids for patient and 
        // sample (if applicable) and import clinical data accordingly
        for (CompositeClinicalData ccd : list) {
            // update internal ids for composte clinical data
            try {
                ccd = updateCompositeData(ccd);
            }
            catch (NullPointerException ex) {
                LOG.error("Could not update composite clinical data for patient, sample: " + 
                        ccd.getPatient().getStableId() + ", " + ccd.getSample().getStableId());
                ex.printStackTrace();
                continue;
            }
            // add case id to case id set
            caseIdSet.add(ccd.getSample().getInternalId());
            
            if (!ccd.getPatientClinicalData().isEmpty()) {
                // init patient clinical data added map with existing clinical data
                if (!patientClinicalDataAdded.containsKey(ccd.getPatient().getInternalId())) {
                    patientClinicalDataAdded.put(ccd.getPatient().getInternalId(), 
                            clinicalDataJdbcDaoImpl.getPatientClinicalDataAttributes(ccd.getPatient().getInternalId()));
                }
                // get existing clinical data and current composite clinical data
                Map<String, String> existingClinicalData = patientClinicalDataAdded.get(ccd.getPatient().getInternalId());
                Map<String, String> filteredClinicalData = filterClinicalAttributes(existingClinicalData, ccd, "patient");
                
                // filter composite patient clinical data by whether patientClinicalAttributes contains attr id or not
                Predicate<String> attrIdFilter = (String attrId) -> filteredClinicalData.keySet().contains(attrId);
                Map<String, PatientClinicalData> newPatientClinicalData = Maps.filterKeys(ccd.getPatientClinicalData(), attrIdFilter);
                
                // update patient clinical data added map and patient clinical data list for batch importing
                existingClinicalData.putAll(filteredClinicalData);
                patientClinicalDataAdded.put(ccd.getPatient().getInternalId(), existingClinicalData);
                patientClinicalData.addAll(newPatientClinicalData.values());
            }
            
            // continue if composite clinical data record doesn't have sample data(i.e., clinical-patient)
            if (ccd.getSample().getInternalId() == -1) {
                continue;
            }
            
            if (!ccd.getSampleClinicalData().isEmpty()) {
                // init sample clinical data added map with existing clinical data
                if (!sampleClinicalDataAdded.containsKey(ccd.getSample().getInternalId())) {
                    sampleClinicalDataAdded.put(ccd.getSample().getInternalId(), 
                            clinicalDataJdbcDaoImpl.getSampleClinicalDataAttributes(ccd.getSample().getInternalId()));
                }
                // get existing clinical data and current composite clinical data
                Map<String, String> existingClinicalData = sampleClinicalDataAdded.get(ccd.getSample().getInternalId());
                Map<String, String> filteredClinicalData = filterClinicalAttributes(existingClinicalData, ccd, "sample");

                // filter composite sample clinical data by whether sampleClinicalAttributes contains attr id or not
                Predicate<String> attrIdFilter = (String attrId) -> filteredClinicalData.keySet().contains(attrId);
                Map<String, SampleClinicalData> newSampleClinicalData = Maps.filterKeys(ccd.getSampleClinicalData(), attrIdFilter);
                
                // update sample clinical data added map and sample clinical data list for batch importing
                existingClinicalData.putAll(filteredClinicalData);
                sampleClinicalDataAdded.put(ccd.getSample().getInternalId(), existingClinicalData);
                sampleClinicalData.addAll(newSampleClinicalData.values());
            }
        }
    
        // import batch of patient clinical data
        if (!patientClinicalData.isEmpty()) {
            int rowsAffected = clinicalDataJdbcDaoImpl.addClinicalDataBatch("clinical_patient", patientClinicalData);
            this.patientDataCount += rowsAffected;                
        }
        
        // import batch of sample clinical data
        if (!sampleClinicalData.isEmpty()) {
            int rowsAffected = clinicalDataJdbcDaoImpl.addClinicalDataBatch("clinical_sample", sampleClinicalData);
            this.sampleDataCount += rowsAffected;
        }
    }
    
    /**
     * Update patient and sample internal ids in composite clinical data.
     * 
     * @param composite
     * @return CompositeClinicalData
     */
    private CompositeClinicalData updateCompositeData(CompositeClinicalData composite) throws Exception {
        // update patient internal id in composite clinical data object
        int patientInternalId = composite.getPatient().getInternalId();
        if (composite.getPatient().getInternalId() != -1) {
            composite.updatePatientInternalId(patientInternalId);
        }            
        else {
            Patient existingPatient = patientJdbcDaoImpl.getPatient(composite.getPatient().getStableId(), composite.getPatient().getCancerStudyId());
            if (existingPatient != null) {
                composite.updatePatientInternalId(existingPatient.getInternalId());
            }
            else {
                Patient newPatient = patientJdbcDaoImpl.addPatient(composite.getPatient());
                composite.updatePatientInternalId(newPatient.getInternalId());
                this.patientCount++;
            }
        }

        // update sample internal id in composite clinical data object
        if (!Strings.isNullOrEmpty(composite.getSample().getStableId())) {
            int sampleInternalId = composite.getSample().getInternalId();
            if (sampleInternalId != -1) {
                composite.updateSampleInternalId(sampleInternalId);
            }
            else {
                Sample existingSample;
                if (composite.getPatient().getInternalId() != -1) {
                    existingSample = sampleJdbcDaoImpl.getSampleByPatient(composite.getSample().getStableId(), composite.getPatient().getInternalId());
                }
                else {
                    existingSample = sampleJdbcDaoImpl.getSampleByStudy(composite.getSample().getStableId(), composite.getPatient().getCancerStudyId());
                }
                if (existingSample != null) {
                    sampleInternalId = existingSample.getInternalId();
                }
                else {
                    Sample newSample = sampleJdbcDaoImpl.addSample(composite.getSample());
                    sampleInternalId = newSample.getInternalId();
                    this.sampleCount++;
                }
                composite.updateSampleInternalId(sampleInternalId);
            }
        }
        
        return composite;
    }
    
    /**
     * Filter out existing clinical attributes for composite clinical data. 
     * 
     * @param existingClinicalAttributes
     * @param attributeType
     * @param composite
     * @return Set<String>
     */
    private Map<String, String> filterClinicalAttributes(Map<String, String> existingClinicalData, CompositeClinicalData composite, String attributeType) {
        // get the stable id and set of clinical attributes by attribute type
        String stableId;
        Map<String, String> compositeClinicalData = new HashMap<>();
        if (attributeType.equals("patient")) {
            stableId = composite.getPatient().getStableId();
            composite.getPatientClinicalData().values().stream().forEach((pcd) -> {
                compositeClinicalData.put(pcd.getAttrId(), pcd.getAttrValue());
            });
        }
        else {
            stableId = composite.getSample().getStableId();
            composite.getSampleClinicalData().values().stream().forEach((scd) -> {
                compositeClinicalData.put(scd.getAttrId(), scd.getAttrValue());
            });
        }
        
        // get intersection of existing clinical attributes and clinical attributes
        // from composite object and remove them from composite clinical data
        Set<String> duplicateAttributes = new HashSet(existingClinicalData.keySet());
        duplicateAttributes.retainAll(compositeClinicalData.keySet());
        if (!duplicateAttributes.isEmpty()) {
            for (String attr : duplicateAttributes) {
                // get the attribute value from composite clinical data and 
                // compare to existing value
                String value = compositeClinicalData.get(attr);
                String existingValue = existingClinicalData.get(attr);
                if (!value.equals(existingValue)) {
                    LOG.warn("Clinical data for " + attributeType + " " + stableId + " already loaded as " + 
                            attr + "=" + existingValue + " - skipping import for " + attr + "=" + value);
                }
            }
        }
        // filter composite clinical data map
        Set<String> filteredAttrIds = compositeClinicalData.keySet();
        filteredAttrIds.removeAll(existingClinicalData.keySet());
        Predicate<String> attrIdFilter = (String attrId) -> filteredAttrIds.contains(attrId);
        Map<String, String> filteredCompositeClinicalData = Maps.filterKeys(compositeClinicalData, attrIdFilter);
        
        return filteredCompositeClinicalData;
    }
        
}

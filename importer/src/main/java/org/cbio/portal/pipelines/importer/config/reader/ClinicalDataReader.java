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

package org.cbio.portal.pipelines.importer.config.reader;

import org.mskcc.cbio.model.*;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;
import org.cbio.portal.pipelines.importer.config.composite.CompositeClinicalData;

import java.util.*;
import java.io.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.*;
import org.springframework.batch.item.file.transform.*;
import org.springframework.core.io.FileSystemResource;

/**
 *
 * @author ochoaa
 */
public class ClinicalDataReader implements ItemStreamReader<CompositeClinicalData> {
    
    private CancerStudy cancerStudy;
    private List<CompositeClinicalData> compositeClinicalDataResults;
    
    private static final Log LOG = LogFactory.getLog(ClinicalDataReader.class);    
    
    public static enum MissingAttributeValues {
        NOT_APPLICABLE("Not Applicable"),
        NOT_AVAILABLE("Not Available"),
        PENDING("Pending"),
        DISCREPANCY("Discrepancy"),
        COMPLETED("Completed"),
        NULL("null"),
        MISSING(""),
        NA("NA"),
        N_A("N/A"),
        UNKNOWN("unknown");

        private String propertyName;
        
        MissingAttributeValues(String propertyName) { this.propertyName = propertyName; }
        @Override
        public String toString() { return propertyName; }

        static public boolean has(String value) {
            if (value == null) return false;
            if (value.trim().equals("")) return true;
            try { 
                value = value.replaceAll("[\\[|\\]\\/]", "");
                value = value.replaceAll(" ", "_");
                return valueOf(value.toUpperCase()) != null;
            }
            catch (IllegalArgumentException ex) {
                return false;
            }
        }

        static public String getNotAvailable() {
            return "[" + NOT_AVAILABLE.toString() + "]";
        }
    }
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        List<File> dataFileList = (List<File>) executionContext.get("dataFileList");
        MultiKeyMap clinicalMetadata = (MultiKeyMap) executionContext.get("clinicalMetadata");
        
        // for each file, read in the clinical data 
        List<CompositeClinicalData> compositeClinicalDataList = new ArrayList();
        dataFileList.stream().forEach((dataFile) -> {
            List<CompositeClinicalData> compositeClinicalData = new ArrayList();
            String[] header = (String[]) clinicalMetadata.get(dataFile.getName(), "header");
            List<ClinicalAttribute> clinicalAttributes = (List<ClinicalAttribute>) clinicalMetadata.get(dataFile.getName(), "clinicalAttributes");
            try {
                compositeClinicalData = loadClinicalData(dataFile, header, clinicalAttributes);
            } catch (Exception ex) {
                LOG.error("Error loading clinical data from: " + dataFile.getName());
                ex.printStackTrace();
            }
            // report number of records loaded from clinical file
            if (!compositeClinicalData.isEmpty()) {
                LOG.info("Clinical records loaded from: " + dataFile.getName() + ": " + compositeClinicalData.size());
                compositeClinicalDataList.addAll(compositeClinicalData);
            }
        });        
        this.compositeClinicalDataResults = compositeClinicalDataList;
    }
    
    /**
     * Load clinical data from data file.
     * 
     * @param dataFile
     * @param header
     * @param clinicalAttributes
     * @return List<CompositeClinicalData>
     */
    private List<CompositeClinicalData> loadClinicalData(File dataFile, String[] header, List<ClinicalAttribute> clinicalAttributes) throws Exception {
        // init tab-delim tokenizer with names of clinical attributes
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(header);
        
        // init line mapper for clinical data file
        DefaultLineMapper<CompositeClinicalData> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(clinicalFieldSetMapper(clinicalAttributes));
        
        // set up clinical data file reader context
        FlatFileItemReader<CompositeClinicalData> clinicalDataReader = new FlatFileItemReader();
        clinicalDataReader.setResource(new FileSystemResource(dataFile));        
        clinicalDataReader.setLineMapper(lineMapper);
        clinicalDataReader.setLinesToSkip(1);
        clinicalDataReader.open(new ExecutionContext());

        // read through each record in clinical data file
        List<CompositeClinicalData> compositeClinicalData = new ArrayList();
        CompositeClinicalData record = clinicalDataReader.read();
        while (record != null) {
            compositeClinicalData.add(record);
            record = clinicalDataReader.read();
        }
        clinicalDataReader.close();

        return compositeClinicalData;
    }

    /**
     * Returns a field set mapper for the clinical data file.
     * 
     * @param dataFilename
     * @param hasSampleIdCol
     * @return FieldSetMapper
     */
    private FieldSetMapper clinicalFieldSetMapper(List<ClinicalAttribute> clinicalAttributes) {     
        return (FieldSetMapper) (FieldSet fs) -> {            
            // get stable ids from record
            String patientStableId = DataFileUtils.getPatientStableId(fs.readString("PATIENT_ID"));
            String sampleStableId = fs.getProperties().containsKey("SAMPLE_ID")?
                    DataFileUtils.getSampleStableId(fs.readString("SAMPLE_ID")):"";
            if (DataFileUtils.isNullOrEmptyValue(patientStableId) && !DataFileUtils.isNullOrEmptyValue(sampleStableId)) {
                patientStableId = sampleStableId;
            }
            
            // create new patient 
            Patient patient = new Patient();
            patient.setStableId(patientStableId);
            patient.setCancerStudyId(cancerStudy.getCancerStudyId());
            patient.setInternalId(-1);

            // create new sample
            Sample sample = new Sample();
            sample.setStableId(sampleStableId);
            sample.setPatientId(patient.getInternalId());
            sample.setTypeOfCancerId(cancerStudy.getTypeOfCancerId());
            sample.setInternalId(-1);
                        
            // resolve sample type, default is PRIMARY_SOLID_TUMOR 
            // sample type will be corrected if sample already exists by ClinicalDataWriter
            Sample.SampleType sampleType = Sample.SampleType.PRIMARY_SOLID_TUMOR;
            if (sample.getInternalId() == -1 && fs.getProperties().containsKey("SAMPLE_TYPE")) {
                String sampleTypeString = DataFileUtils.getSampleTypeString(sample.getStableId(), fs.readRawString("SAMPLE_TYPE"));
                try {
                    sampleType = Sample.SampleType.valueOf(sampleTypeString);
                }
                catch (IllegalArgumentException ex) {}
            }
            sample.setSampleType(sampleType);

            // if attr value not "missing" then add to filtered clinical data map
            Map<ClinicalAttribute, String> filteredClinicalData = new HashMap<>();
            clinicalAttributes.stream().forEach((attr) -> {
                String attrVal = fs.readString(attr.getAttrId());
                if (!(MissingAttributeValues.has(attrVal))) {
                    // truncate attribute value to prevent MysqlDataTruncation exceptions
                    if (attrVal.length() > DataFileUtils.DATA_TRUNCATION_THRESHOLD) {
                        attrVal = attrVal.substring(0, DataFileUtils.DATA_TRUNCATION_THRESHOLD);
                    }
                    filteredClinicalData.put(attr, attrVal);
                }
            });
            CompositeClinicalData composite = new CompositeClinicalData();
            composite.setPatient(patient);
            composite.setSample(sample);
            composite.setCompositeClinicalDataMap(filteredClinicalData);
            
            return composite;
        };
    }
    
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}    

    @Override
    public CompositeClinicalData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!compositeClinicalDataResults.isEmpty()) {
            return compositeClinicalDataResults.remove(0);
        }
        return null;
    }
    
}

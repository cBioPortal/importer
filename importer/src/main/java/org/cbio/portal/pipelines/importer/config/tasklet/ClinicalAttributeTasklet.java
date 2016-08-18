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

package org.cbio.portal.pipelines.importer.config.tasklet;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.ClinicalAttributeJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;

import java.io.*;
import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.*;

/**
 *
 * @author ochoaa
 */
public class ClinicalAttributeTasklet implements Tasklet {
    
    @Autowired
    ClinicalAttributeJdbcDaoImpl clinicalAttributeJdbcDaoImpl;
    
    private final Set<String> IDENTIFYING_ATTRIBUTES = new HashSet<>(Arrays.asList(new String[]{"PATIENT_ID", "SAMPLE_ID"}));
    
    private CancerStudy cancerStudy;
    private int newClinicalAttributes;
    
    private static final Log LOG = LogFactory.getLog(ClinicalAttributeTasklet.class);
    
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        this.cancerStudy = (CancerStudy) chunkContext.getStepContext().getJobExecutionContext().get("cancerStudy");
        
        // get datatype and metafile properties from datatype metadata in execution context
        MultiKeyMap datatypeMetadata = (MultiKeyMap) chunkContext.getStepContext().getJobExecutionContext().get("datatypeMetadata");
        String datatype = (String) chunkContext.getStepContext().getJobExecutionContext().get("currentDatatype");
        Properties properties = (Properties) datatypeMetadata.get(datatype, "properties");
        
        // resolve attribute type from metafile properties - default is SAMPLE
        String attributeType = "SAMPLE";
        if (properties.getProperty("datatype").equals("PATIENT_ATTRIBUTES")) {
            attributeType = "PATIENT";
        }
        
        // load clinical attributes and data file metadata 
        MultiKeyMap clinicalMetadata = new MultiKeyMap();
        List<File> dataFileList = (List<File>) datatypeMetadata.get(datatype, "dataFileList");
        for (File dataFile : dataFileList) {
            LOG.info("Loading clinical attribute meta data from: " + dataFile.getName());
            
            List<ClinicalAttribute> clinicalAttributes = loadClinicalAttributesMetadata(dataFile, attributeType);
            if (!clinicalAttributes.isEmpty()) {
                LOG.info("Loaded " + clinicalAttributes.size() + " clinical attributes from: " + dataFile.getName());                
                clinicalMetadata.putAll(DataFileUtils.loadDataFileMetadata(dataFile));
                clinicalMetadata.put(dataFile.getName(), "clinicalAttributes", clinicalAttributes);
            }
            else {
                LOG.error("Could not load any clinical attributes from: " + dataFile.getName());
            }
        }
        // add data file clinical attributes to the execution context
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("newClinicalAttributes", newClinicalAttributes);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("attributeType", attributeType);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("clinicalMetadata", clinicalMetadata);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("dataFileList", dataFileList);

        return RepeatStatus.FINISHED; 
    }

    /**
     * Load clinical attributes meta data from input file.
     * 
     * @param clinicalDataFile
     * @param attributeType
     * @return List<ClinicalAttribute>
     */
    private List<ClinicalAttribute> loadClinicalAttributesMetadata(File clinicalDataFile, String attributeType) throws Exception {        
        // load clinical attribute metadata from file
        List<ClinicalAttribute> clinicalAttributes = new ArrayList();
        try (FileReader reader = new FileReader(clinicalDataFile)) {
            BufferedReader buff = new BufferedReader(reader);
            String line = buff.readLine();
            String[] displayNames = DataFileUtils.splitDataFields(line);
            String[] descriptions, datatypes, priorities, colnames;
            if (line.startsWith(DataFileUtils.METADATA_PREFIX)) {
                descriptions = DataFileUtils.splitDataFields(buff.readLine());
                datatypes = DataFileUtils.splitDataFields(buff.readLine());
                priorities = DataFileUtils.splitDataFields(buff.readLine());
            }
            else {
                colnames = displayNames;
                descriptions = new String[colnames.length];
                Arrays.fill(descriptions, DataFileUtils.DEFAULT_DESCRIPTION);
                datatypes = new String[colnames.length];
                Arrays.fill(datatypes, DataFileUtils.DEFAULT_DATATYPE);
                priorities = new String[colnames.length];
                Arrays.fill(priorities, DataFileUtils.DEFAULT_PRIORITY);
            }   
            
            // fill in attribute types and get the column names            
            String[] attributeTypes = new String[displayNames.length];
            line = buff.readLine();
            // if next line starts with metadata prefix then fill in attribute types
            if (line.startsWith(DataFileUtils.METADATA_PREFIX)) {                
                attributeTypes = DataFileUtils.splitDataFields(line);
                colnames = DataFileUtils.splitDataFields(buff.readLine());
            }
            else {
                // set line as column names if not starts with metadata prefix
                // and fill attribute types by current clinical attribute type
                // (PATIENT for clinical-patient, SAMPLE for clinical-sample)
                // or assume SAMPLE for attribute type "MIXED" unless attribute
                // exists in db as PATIENT attribute type
                colnames = DataFileUtils.splitDataFields(line);
                if (attributeType.equals("MIXED")) {
                    Arrays.fill(attributeTypes, "SAMPLE");
                }
                else {
                    Arrays.fill(attributeTypes, attributeType);
                }
            }

            int newClinAttrsAdded = 0;
            for (int i=0; i<colnames.length; i++) {
                ClinicalAttribute attr = new ClinicalAttribute();
                attr.setAttrId(colnames[i].trim().toUpperCase());
                attr.setDisplayName(displayNames[i]);
                attr.setDescription(descriptions[i]);
                attr.setDatatype(datatypes[i]);
                attr.setPatientAttribute(attributeTypes[i].equals("PATIENT"));
                attr.setPriority(priorities[i]);
                attr.setCancerStudyId(cancerStudy.getCancerStudyId());
                
                // skip PATIENT_ID and SAMPLE_ID columns
                if (IDENTIFYING_ATTRIBUTES.contains(attr.getAttrId())) {
                    continue;
                }
                
                // check if clinical attribute exists already in db for study
                ClinicalAttribute existingAttr = clinicalAttributeJdbcDaoImpl.getClinicalAttribute(attr.getAttrId(), cancerStudy.getCancerStudyId());
                if (existingAttr != null) {
                    clinicalAttributes.add(existingAttr);
                }
                else {
                    // we are assuming that clinical data has already passed validation and that
                    // attribute type doesn't need to be corrected
                    // skip clinical attributes for patient/sample identifiers
                    if (existingAttr == null) {
                        LOG.info("Importing new " + attributeTypes[i].toLowerCase() +"-level clinical attribute for study: " + attr.getAttrId());
                        clinicalAttributeJdbcDaoImpl.addClinicalAttribute(attr);
                        newClinAttrsAdded++;
                    }
                    clinicalAttributes.add(attr);
                }
            }
            this.newClinicalAttributes += newClinAttrsAdded;
            reader.close();
        }
        
        return clinicalAttributes;
    }

}

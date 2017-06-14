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
import org.mskcc.cbio.persistence.jdbc.*;
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.model.TimelineRecord;

import java.io.*;
import java.util.*;
import org.apache.commons.logging.*;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.collections.map.MultiKeyMap;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.*;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.core.io.FileSystemResource;

/**
 *
 * @author ochoaa
 */
public class TimelineDataReader implements ItemStreamReader<TimelineRecord> {

    @Autowired
    PatientJdbcDaoImpl patientJdbcDaoImpl;        
    
    private final Map<String, String> timelineStagingDataMap = new TimelineRecord().getTimelineStagingDataMap();
    private final Set<String> patientsSkipped = new HashSet<>();
    
    private CancerStudy cancerStudy;
    private List<TimelineRecord> timelineRecordResults;
        
    private static final Log LOG = LogFactory.getLog(TimelineDataReader.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        List<File> dataFileList = (List<File>) executionContext.get("dataFileList");
        MultiKeyMap timelineMetadata = (MultiKeyMap) executionContext.get("timelineMetadata");
        
        // load timeline records from each datafile
        List<TimelineRecord> timelineRecordList = new ArrayList();        
        dataFileList.stream().forEach((dataFile) -> {            
            String[] timelineHeader = (String[]) timelineMetadata.get(dataFile.getName(), "header");                        
            List<TimelineRecord> timelineRecords = new ArrayList();
            try {
                LOG.info("Loading timeline data from: " + dataFile.getName());                
                timelineRecords = loadTimelineRecords(dataFile, timelineHeader);
            } 
            catch (Exception ex) {
                ex.printStackTrace();
            }
            
            if (timelineRecords.isEmpty()) {
                LOG.error("Error loading timeline data from: " + dataFile.getName());
            }
            else {
                LOG.info("Timeline records loaded from: " + dataFile.getName() + ": " + timelineRecords.size());
                timelineRecordList.addAll(timelineRecords);
            }
        });
        // add total patients skipped to execution context for summary statistics after step executes
        executionContext.put("patientsSkipped", patientsSkipped.size());
        
        this.timelineRecordResults = timelineRecordList;        
    }
    
    /**
     * Loads timeline data from the datafile.
     * 
     * @param dataFile
     * @return List<TimelineRecord>
     */
    private List<TimelineRecord> loadTimelineRecords(File dataFile, String[] timelineHeader) throws Exception {        
        // init tab-delim tokenizer with the timeline file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(timelineHeader);
        
        // init line mapper for timeline file
        DefaultLineMapper<TimelineRecord> lineMapper = new DefaultLineMapper<>();        
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(timelineFieldSetMapper());
        
        // set up timeline file reader context
        FlatFileItemReader<TimelineRecord> timelineDataReader = new FlatFileItemReader();
        timelineDataReader.setResource(new FileSystemResource(dataFile));
        timelineDataReader.setLineMapper(lineMapper);
        timelineDataReader.setLinesToSkip(1);
        timelineDataReader.open(new ExecutionContext());
        
        List<TimelineRecord> timelineRecordList = new ArrayList();
        TimelineRecord record = timelineDataReader.read();
        while (record != null) {
            TimelineRecord screenedRecord = screenTimelineRecord(record);
            if (screenedRecord != null) {
                timelineRecordList.add(record);
            }            
            record = timelineDataReader.read();
        }
        timelineDataReader.close();

        return timelineRecordList;
    }
    
    /**
     * Field set mapper for timeline records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper timelineFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));
            Set<String> intersection = Sets.intersection(timelineStagingDataMap.keySet(), fieldsetNames);

            // generate hashmap of clinical event data using the set difference 
            // between properties and expected timeline staging file columns
            Set<String> clinicalEventDataColumns = Sets.difference(fieldsetNames, intersection);
            Map<String, String> clinicalEventDataMap = new HashMap<>();
            clinicalEventDataColumns.stream().forEach((dataColumn) -> {
                String value = fs.readString(dataColumn);
                if (!DataFileUtils.isNullOrEmptyValue(value)) {
                        clinicalEventDataMap.put(dataColumn, value);
                }
            });
            
            BeanMap timelineRecordBeanMap = BeanMap.create(new TimelineRecord());
            // fill property values for bean using the set intersection of properties 
            // and expected timeline staging file columns
            intersection.stream().forEach((column) -> {
                timelineRecordBeanMap.put(timelineStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            TimelineRecord record = (TimelineRecord) timelineRecordBeanMap.getBean();
            record.setClinicalEventDataMap(clinicalEventDataMap);
            
            return record;
        };
    }

    /**
     * Performs basic data screening to determine whether timeline record is acceptable or not.
     * Returns null if timeline record does not pass screening
     * 
     * @param timelineRecord
     * @return TimelineRecord
     */
    private TimelineRecord screenTimelineRecord(TimelineRecord timelineRecord) {
        // make sure that patient can be found in the database by stable id, cancer study id
        String patientStableId = DataFileUtils.getPatientStableId(timelineRecord.getPatientId());
        Patient patient = patientJdbcDaoImpl.getPatient(patientStableId, cancerStudy.getCancerStudyId());
        if (patient == null) {
            if (patientsSkipped.add(patientStableId)) {
                LOG.warn("Could not find patient in db: " + timelineRecord.getPatientId());
            }
            return null;
        }
        // update patient stable id and patient internal id for timeline record
        timelineRecord.setPatientId(patientStableId);
        timelineRecord.setPatientInternalId(patient.getInternalId());
        
        return timelineRecord;
    }
    
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public TimelineRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!timelineRecordResults.isEmpty()) {
            return timelineRecordResults.remove(0);
        }
        return null;
    }
    
}

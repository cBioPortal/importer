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
import org.mskcc.cbio.persistence.jdbc.SampleJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.model.CopyNumberSegmentRecord;
import org.cbio.portal.pipelines.importer.util.*;

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
public class CopyNumberSegmentDataReader implements ItemStreamReader<CopyNumberSegmentRecord> {
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;

    private final Map<String, String> copyNumberSegmentStagingDataMap = new CopyNumberSegmentRecord().getCopyNumberSegmentStagingDataMap();
    private final Set<String> samplesSkipped = new HashSet<>();

    private CancerStudy cancerStudy;
    private List<CopyNumberSegmentRecord> copyNumberSegmentDataResults;
    
    private static final Log LOG = LogFactory.getLog(CopyNumberSegmentDataReader.class);

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        File dataFile = (File) executionContext.get("dataFile");
        MultiKeyMap copyNumberSegmentMetadata = (MultiKeyMap) executionContext.get("copyNumberSegmentMetadata");
        String[] header = (String[]) copyNumberSegmentMetadata.get(dataFile.getName(), "header");
        Integer numRecords = (Integer) copyNumberSegmentMetadata.get(dataFile.getName(), "numRecords");
        
        // load copy number segment data from datafile
        List<CopyNumberSegmentRecord> copyNumberSegmentRecordList = new ArrayList();
        try {
            LOG.info("Loading copy number segment data from: " + dataFile);
            copyNumberSegmentRecordList = loadCopyNumberSegmentData(dataFile, header, numRecords);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        
        // log whether data was loaded or not from copy number segment datafile
        if (copyNumberSegmentRecordList.isEmpty()) {
            LOG.error("Error loading copy number segment data from: " + dataFile.getName());
        }
        else {
            LOG.info("Loaded " + copyNumberSegmentRecordList.size() + "/" + numRecords + " from: " + dataFile.getName());
        }        
        // add total samples skipped to execution context for summary statistics after step executes
        executionContext.put("samplesSkipped", samplesSkipped.size());

        this.copyNumberSegmentDataResults = copyNumberSegmentRecordList;
    }
    
    /**
     * Loads copy number segment data from the datafile.
     * 
     * @param dataFile
     * @return List<CopyNumberSegmentRecord>
     */
    private List<CopyNumberSegmentRecord> loadCopyNumberSegmentData(File dataFile, String[] copyNumberSegmentHeader, Integer numRecords) throws Exception {
        // init tab-delim tokenizer with the copy number segment file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(copyNumberSegmentHeader);
        
        // init line mapper for copy number segment file
        DefaultLineMapper<CopyNumberSegmentRecord> lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(copyNumberSegmentFieldSetMapper());
        
        // set up copy number segment file reader context
        FlatFileItemReader<CopyNumberSegmentRecord> copyNumberSegmentDataReader = new FlatFileItemReader();
        copyNumberSegmentDataReader.setResource(new FileSystemResource(dataFile));
        copyNumberSegmentDataReader.setLineMapper(lineMapper);
        copyNumberSegmentDataReader.setLinesToSkip(1);
        copyNumberSegmentDataReader.open(new ExecutionContext());
        
        List<CopyNumberSegmentRecord> copyNumberSegmentRecordList = new ArrayList();
        CopyNumberSegmentRecord record = copyNumberSegmentDataReader.read();
        while (record != null) {
            CopyNumberSegmentRecord copyNumberSegmentRecord = screenCopyNumberSegmentRecord(record);
            if (copyNumberSegmentRecord != null) {
                copyNumberSegmentRecordList.add(copyNumberSegmentRecord);
            }
            record = copyNumberSegmentDataReader.read();
        }
        copyNumberSegmentDataReader.close();
        
        return copyNumberSegmentRecordList;
    }
    
    /**
     * Field set mapper for copy number segment records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper copyNumberSegmentFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {            
            BeanMap copyNumberSegmentRecordBeanMap = BeanMap.create(new CopyNumberSegmentRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));

            // fill property values for bean using intersection of properties and expected copy number segment columns
            Set<String> intersection = Sets.intersection(copyNumberSegmentStagingDataMap.keySet(), fieldsetNames);
            intersection.stream().forEach((column) -> {
                copyNumberSegmentRecordBeanMap.put(copyNumberSegmentStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            CopyNumberSegmentRecord record = (CopyNumberSegmentRecord) copyNumberSegmentRecordBeanMap.getBean();
            
            return record;
        };
    }
    
    /**
     * Performs basic data screening to determine whether copy number segment record is acceptable or not.
     * Returns null if copy number segment record does not pass screening
     * 
     * @param copyNumberSegmentRecord
     * @return CopyNumberSegmentRecord
     */
    private CopyNumberSegmentRecord screenCopyNumberSegmentRecord(CopyNumberSegmentRecord copyNumberSegmentRecord) {
        // make sure that sample can be found in the database by stable id, cancer study id
        String sampleStableId = DataFileUtils.getSampleStableId(copyNumberSegmentRecord.getId());
        Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, cancerStudy.getCancerStudyId());
        if (sample == null) {
            if (samplesSkipped.add(sampleStableId)) {
                if (!DataFileUtils.isNormalSample(sampleStableId)) {
                    LOG.warn("Could not find sample in db: " + copyNumberSegmentRecord.getId());
                }
                else {
                    LOG.warn("Skipping normal sample: " + sampleStableId);
                }
            }
            return null;
        }
        // update record with sample id and cancer study id
        copyNumberSegmentRecord.setCancerStudyId(cancerStudy.getCancerStudyId());
        copyNumberSegmentRecord.setSampleId(sample.getInternalId());
        
        return copyNumberSegmentRecord;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public CopyNumberSegmentRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!copyNumberSegmentDataResults.isEmpty()) {
            return copyNumberSegmentDataResults.remove(0);
        }
        return null;
    }
    
}

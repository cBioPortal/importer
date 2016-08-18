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
import org.cbio.portal.pipelines.importer.model.FusionRecord;

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
public class FusionDataReader implements ItemStreamReader<FusionRecord> {

    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    private final Map<String, String> fusionStagingDataMap = new FusionRecord().getFusionStagingData();
    private final Set<String> samplesSkipped = new HashSet<>();
    
    private GeneticProfile geneticProfile;    
    private List<FusionRecord> fusionRecordResults;
    
    private static final Log LOG = LogFactory.getLog(FusionDataReader.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.geneticProfile = (GeneticProfile) executionContext.get("geneticProfile");
        List<File> dataFileList = (List<File>) executionContext.get("dataFileList");
        MultiKeyMap fusionFileMetadata = (MultiKeyMap) executionContext.get("mutationFileMetadata");
        
        // load fusion records from each data file in list
        List<FusionRecord> fusionRecordList = new ArrayList();
        dataFileList.stream().forEach((dataFile) -> {            
            String[] fusionHeader = (String[]) fusionFileMetadata.get(dataFile.getName(), "header");
            int numRecords = (int) fusionFileMetadata.get(dataFile.getName(), "numRecords");
            
            // load fusion records from each datafile
            List<FusionRecord> fusionRecords = new ArrayList();
            try {
                LOG.info("Loading fusion data from: " + dataFile.getName());                
                fusionRecords = loadFusionRecords(dataFile, fusionHeader);
            } 
            catch (Exception ex) {
                ex.printStackTrace();
            }
            
            if (fusionRecords.isEmpty()) {
                LOG.error("Error loading fusion data from: " + dataFile.getName());
            }
            else {
                LOG.info("Loaded " + fusionRecords.size() + "/" + numRecords + " from: " + dataFile.getName());
                fusionRecordList.addAll(fusionRecords);
            }
        });        
        // add counts to execution context for summary statistics after step executes
        executionContext.put("samplesSkipped", samplesSkipped.size());
        executionContext.put("isMutationDatatype", false);

        this.fusionRecordResults = fusionRecordList;
    }

    /**
     * Loads fusion data from the datafile.
     * 
     * @param dataFile
     * @return List<FusionRecord>
     */
    private List<FusionRecord> loadFusionRecords(File dataFile, String[] fusionHeader) throws Exception {        
        // init tab-delim tokenizer with the fusion file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(fusionHeader);
        
        // init line mapper for fusion file
        DefaultLineMapper<FusionRecord> lineMapper = new DefaultLineMapper<>();        
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fusionFieldSetMapper());
        
        // set up fusion file reader context
        FlatFileItemReader<FusionRecord> fusionDataReader = new FlatFileItemReader();
        fusionDataReader.setResource(new FileSystemResource(dataFile));
        fusionDataReader.setLineMapper(lineMapper);
        fusionDataReader.setLinesToSkip(1);
        fusionDataReader.open(new ExecutionContext());
        
        List<FusionRecord> fusionRecordList = new ArrayList();
        FusionRecord record = fusionDataReader.read();
        while (record != null) {
            FusionRecord screenedRecord = screenFusionRecord(record);
            if (screenedRecord != null) {
                fusionRecordList.add(screenedRecord);
            }
            record = fusionDataReader.read();
        }
        fusionDataReader.close();

        return fusionRecordList;
    }
    
    /**
     * Field set mapper for fusion records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper fusionFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {
            BeanMap fusionRecordBeanMap = BeanMap.create(new FusionRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));

            // fill property values for bean using intersection of properties and expected fusion columns
            Set<String> intersection = Sets.intersection(fusionStagingDataMap.keySet(), fieldsetNames);            
            intersection.stream().forEach((column) -> {
                fusionRecordBeanMap.put(fusionStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            FusionRecord record = (FusionRecord) fusionRecordBeanMap.getBean();
            
            return record;
        };
    }
    
    /**
     * Performs basic data screening to determine whether fusion record is acceptable or not.
     * Returns null if fusion record does not pass screening
     * 
     * @param fusionRecord
     * @return FusionRecord
     */
    private FusionRecord screenFusionRecord(FusionRecord fusionRecord) {
        // make sure that sample can be found in the database by stable id, cancer study id
        String sampleStableId = DataFileUtils.getSampleStableId(fusionRecord.getTumorSampleBarcode());
        Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, geneticProfile.getCancerStudyId());
        if (sample == null) {
            if (samplesSkipped.add(sampleStableId)) {
                if (!DataFileUtils.isNormalSample(sampleStableId)) {
                    LOG.warn("Could not find sample in db: " + fusionRecord.getTumorSampleBarcode());
                }
                else {
                    LOG.warn("Skipping normal sample: " + sampleStableId);
                }
            }
            return null;
        }
        // update sample id for fusion record
        fusionRecord.setSampleId(sample.getInternalId());
        fusionRecord.setGeneticProfileId(geneticProfile.getGeneticProfileId());
        
        return fusionRecord;
    }
    
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public FusionRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!fusionRecordResults.isEmpty()) {
            return fusionRecordResults.remove(0);
        }
        return null;
    }
    
}

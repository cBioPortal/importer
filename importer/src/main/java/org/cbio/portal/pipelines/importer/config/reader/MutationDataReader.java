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
import org.cbio.portal.pipelines.importer.model.MafRecord;

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
public class MutationDataReader implements ItemStreamReader<MafRecord> {
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    private final Map<String, String> mafStagingDataMap = new MafRecord().getMafStagingDataMap();
    private final Set<String> samplesSkipped = new HashSet<>();
    
    private GeneticProfile geneticProfile;
    private List<MafRecord> mafRecordResults;
    
    private static final Log LOG = LogFactory.getLog(MutationDataReader.class);    
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.geneticProfile = (GeneticProfile) executionContext.get("geneticProfile");
        List<File> dataFileList = (List<File>) executionContext.get("dataFileList");
        MultiKeyMap mafFileMetadata = (MultiKeyMap) executionContext.get("mutationFileMetadata");
        
        List<MafRecord> mafRecordList = new ArrayList();        
        dataFileList.stream().forEach((dataFile) -> {            
            String[] mafHeader = (String[]) mafFileMetadata.get(dataFile.getName(), "header");
            int numRecords = (int) mafFileMetadata.get(dataFile.getName(), "numRecords");
            
            // load MAF records from each datafile
            List<MafRecord> mafRecords = new ArrayList();
            try {
                LOG.info("Loading mutation data from: " + dataFile.getName());                
                mafRecords = loadMafRecords(dataFile, mafHeader);
            } 
            catch (Exception ex) {
                ex.printStackTrace();
            }
            
            if (mafRecords.isEmpty()) {
                LOG.error("Error loading mutation data from: " + dataFile.getName());
            }
            else {
                LOG.info("Loaded " + mafRecords.size() + "/" + numRecords + " from: " + dataFile.getName());
                mafRecordList.addAll(mafRecords);
            }
        });
        // add counts to execution context for summary statistics after step executes
        executionContext.put("samplesSkipped", samplesSkipped.size());
        executionContext.put("isMutationDatatype", true);
        
        this.mafRecordResults = mafRecordList;        
    }
    
    /**
     * Loads MAF data from the datafile.
     * 
     * @param dataFile
     * @return List<MafRecord>
     */
    private List<MafRecord> loadMafRecords(File dataFile, String[] mafHeader) throws Exception {        
        // init tab-delim tokenizer with the maf file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(mafHeader);
        
        // init line mapper for maf file
        DefaultLineMapper<MafRecord> lineMapper = new DefaultLineMapper<>();        
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mutationFieldSetMapper());
        
        // set up maf file reader context
        FlatFileItemReader<MafRecord> mafDataReader = new FlatFileItemReader();
        mafDataReader.setResource(new FileSystemResource(dataFile));
        mafDataReader.setLineMapper(lineMapper);
        mafDataReader.setLinesToSkip(1);
        mafDataReader.open(new ExecutionContext());
        
        List<MafRecord> mafRecordList = new ArrayList();
        MafRecord record = mafDataReader.read();
        while (record != null) {
            MafRecord screenedRecord = screenMafRecord(record);
            if (screenedRecord != null) {
                mafRecordList.add(screenedRecord);
            }
            record = mafDataReader.read();
        }
        mafDataReader.close();

        return mafRecordList;
    }
    
    /**
     * Field set mapper for MAF records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper mutationFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {
            BeanMap mafRecordBeanMap = BeanMap.create(new MafRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));

            // fill property values for bean using intersection of properties and expected maf columns
            Set<String> intersection = Sets.intersection(mafStagingDataMap.keySet(), fieldsetNames);            
            intersection.stream().forEach((column) -> {
                mafRecordBeanMap.put(mafStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            MafRecord record = (MafRecord) mafRecordBeanMap.getBean();
            
            return record;
        };
    }

    /**
     * Performs basic data screening to determine whether MAF record is acceptable or not.
     * Returns null if MAF record does not pass screening
     * 
     * @param mafRecord
     * @return MafRecord
     */
    private MafRecord screenMafRecord(MafRecord mafRecord) {
        // make sure that sample can be found in the database by stable id, cancer study id
        String sampleStableId = DataFileUtils.getSampleStableId(mafRecord.getTumorSampleBarcode());
        Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, geneticProfile.getCancerStudyId());
        if (sample == null) {
            if (samplesSkipped.add(sampleStableId)) {
                if (!DataFileUtils.isNormalSample(sampleStableId)) {
                    LOG.warn("Could not find sample in db: " + mafRecord.getTumorSampleBarcode());
                }
                else {
                    LOG.warn("Skipping normal sample: " + sampleStableId);
                }
            }
            return null;
        }        
        // update sample id for MAF record since all data screens have passed
        mafRecord.setTumorSampleBarcode(sampleStableId);
        mafRecord.setSampleId(sample.getInternalId());
        mafRecord.setGeneticProfileId(geneticProfile.getGeneticProfileId());
        
        return mafRecord;
    }
    
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public MafRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!mafRecordResults.isEmpty()) {
            return mafRecordResults.remove(0);
        }
        return null;
    }
    
}

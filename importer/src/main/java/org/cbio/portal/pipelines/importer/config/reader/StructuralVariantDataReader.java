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
import org.cbio.portal.pipelines.importer.model.StructuralVariantRecord;

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
public class StructuralVariantDataReader implements ItemStreamReader<StructuralVariantRecord> {

    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    private final Map<String, String> structuralVariantStagingDataMap = new StructuralVariantRecord().getStructuralVariantStagingDataMap();
    private final Set<String> samplesSkipped = new HashSet<>();
    
    private GeneticProfile geneticProfile;
    private List<StructuralVariantRecord> structuralVariantRecordResults;
    
    private static final Log LOG = LogFactory.getLog(StructuralVariantDataReader.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.geneticProfile = (GeneticProfile) executionContext.get("geneticProfile");
        File dataFile = (File) executionContext.get("dataFile");
        MultiKeyMap structuralVariantMetadata = (MultiKeyMap) executionContext.get("structuralVariantMetadata");
        String[] header = (String[]) structuralVariantMetadata.get(dataFile.getName(), "header");
        Integer numRecords = (Integer) structuralVariantMetadata.get(dataFile.getName(), "numRecords");
        
        // load structural variant data from datafile
        List<StructuralVariantRecord> structuralVariantDataList = new ArrayList();
        try {
            LOG.info("Loading structural variant data from: " + dataFile.getName());
            structuralVariantDataList = loadStructuralVariantData(dataFile, header);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        
        // log whether data was loaded or not from structural variant datafile
        if (structuralVariantDataList.isEmpty()) {
            LOG.error("Error loading structural variant data from: " + dataFile.getName());
        }
        else {
            LOG.info("Loaded " + structuralVariantDataList.size() + "/" + numRecords + " records from: " + dataFile.getName());
        }
        // add counts to execution context for summary statistics after step executes
        executionContext.put("samplesSkipped", samplesSkipped.size());
        
        this.structuralVariantRecordResults = structuralVariantDataList;
    }
    
    /**
     * Loads structural variant data from the datafile.
     * 
     * @param dataFile
     * @param structuralVariantHeader
     * @return List<StructuralVariantRecord>
     */
    private List<StructuralVariantRecord> loadStructuralVariantData(File dataFile, String[] structuralVariantHeader) throws Exception {
        // init tab-delim tokenizer with the structural variant file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(structuralVariantHeader);
        
        // init line mapper for structural variant file
        DefaultLineMapper<StructuralVariantRecord> lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(structuralVariantFieldSetMapper());
        
        // set up structural variant file reader context
        FlatFileItemReader<StructuralVariantRecord> structuralVariantDataReader = new FlatFileItemReader();
        structuralVariantDataReader.setResource(new FileSystemResource(dataFile));
        structuralVariantDataReader.setLineMapper(lineMapper);
        structuralVariantDataReader.setLinesToSkip(1);
        structuralVariantDataReader.open(new ExecutionContext());
        
        List<StructuralVariantRecord> structuralVariantDataList = new ArrayList();
        StructuralVariantRecord record = structuralVariantDataReader.read();
        while (record != null) {
            StructuralVariantRecord structuralVariant = screenStructuralVariantRecord(record);
            if (structuralVariant != null) {
                structuralVariantDataList.add(structuralVariant);
            }
            record = structuralVariantDataReader.read();
        }
        structuralVariantDataReader.close();
        
        return structuralVariantDataList;
    }

    /**
     * Field set mapper for structural variant records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper structuralVariantFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {
            BeanMap structuralVariantRecordBeanMap = BeanMap.create(new StructuralVariantRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));
            
            // fill property values for bean using intersection of properties and expected structural variant columns
            Set<String> intersection = Sets.intersection(structuralVariantStagingDataMap.keySet(), fieldsetNames);
            intersection.stream().forEach((column) -> {
                structuralVariantRecordBeanMap.put(structuralVariantStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            StructuralVariantRecord record = (StructuralVariantRecord) structuralVariantRecordBeanMap.getBean();
            return record;
        };
    }
    
    /**
     * Performs basic data screening to determine whether structural variant record is acceptable or not. 
     * Returns null if structural variant record does not pass screening
     * 
     * @param structuralVariantRecord
     * @return StructuralVariantRecord
     */
    private StructuralVariantRecord screenStructuralVariantRecord(StructuralVariantRecord structuralVariantRecord) {
        // make sure that sample can be found in the database by stable id, cancer study id
        String sampleStableId = DataFileUtils.getSampleStableId(structuralVariantRecord.getSampleId());
        Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, geneticProfile.getCancerStudyId());
        if (sample == null) {
            if (samplesSkipped.add(sampleStableId)) {
                if (!DataFileUtils.isNormalSample(sampleStableId)) {
                    LOG.warn("Could not find sample in db: " + structuralVariantRecord.getSampleId());
                }
                else {
                    LOG.warn("Skipping normal sample: " + sampleStableId);
                }
            }
            return null;
        }
        // update structural variant record with sample data and genetic profile id
        structuralVariantRecord.setSampleId(sampleStableId);
        structuralVariantRecord.setSampleInternalId(sample.getInternalId());
        structuralVariantRecord.setGeneticProfileId(geneticProfile.getGeneticProfileId());

        return structuralVariantRecord;
    }
    
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public StructuralVariantRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!structuralVariantRecordResults.isEmpty()) {
            return structuralVariantRecordResults.remove(0);
        }
        return null;
    }
    
}

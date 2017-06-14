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
import org.cbio.portal.pipelines.importer.model.MutSigRecord;

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
import org.springframework.cglib.beans.BeanMap;
import org.springframework.core.io.FileSystemResource;

/**
 *
 * @author ochoaa
 */
public class MutSigDataReader implements ItemStreamReader<MutSigRecord> {
    
    private final Map<String, String> mutSigStagingDataMap = new MutSigRecord().getMutSigStagingDataMap();
    private CancerStudy cancerStudy;
    private List<MutSigRecord> mutSigDataResults;
    
    private static final Log LOG = LogFactory.getLog(MutSigDataReader.class);

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        File dataFile = (File) executionContext.get("dataFile");
        MultiKeyMap mutSigMetadata = (MultiKeyMap) executionContext.get("mutSigMetadata");
        String[] header = (String[]) mutSigMetadata.get(dataFile.getName(), "header");
        Integer numRecords = (Integer) mutSigMetadata.get(dataFile.getName(), "numRecords");
        
        // load mutsig data from datafile
        List<MutSigRecord> mutSigDataList = new ArrayList();
        try {
            LOG.info("Loading mutsig data from: " + dataFile);
            mutSigDataList = loadMutSigData(dataFile, header);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        
        // log whether data was loaded or not from mutsig datafile
        if (mutSigDataList.isEmpty()) {
            LOG.error("Error loading mutsig data from: " + dataFile.getName());
        }
        else {
            LOG.info("Loaded " + mutSigDataList.size() + "/" + numRecords + " from: " + dataFile.getName());
        }
        
        this.mutSigDataResults = mutSigDataList;
    }
    
    /**
     * Loads mutsig data from the datafile.
     * 
     * @param dataFile
     * @param mutSigHeader
     * @return List<MutSigRecord>
     */
    private List<MutSigRecord> loadMutSigData(File dataFile, String[] mutSigHeader) throws Exception {
        // init tab-delim tokenizer with the mutsig file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(mutSigHeader);
        
        // init line mapper for mutsig file
        DefaultLineMapper<MutSigRecord> lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mutSigFieldSetMapper());
        
        // set up mustig file reader context
        FlatFileItemReader<MutSigRecord> mutSigDataReader = new FlatFileItemReader();
        mutSigDataReader.setResource(new FileSystemResource(dataFile));
        mutSigDataReader.setLineMapper(lineMapper);
        mutSigDataReader.setLinesToSkip(1);
        mutSigDataReader.open(new ExecutionContext());
        
        List<MutSigRecord> mutSigDataList = new ArrayList();
        MutSigRecord record = mutSigDataReader.read();
        while (record != null) {
            // update mut sig record cancer study id
            record.setCancerStudyId(cancerStudy.getCancerStudyId());
            mutSigDataList.add(record);
            
            record = mutSigDataReader.read();
        }
        mutSigDataReader.close();
        
        return mutSigDataList;
    }
    
    /**
     * Field set mapper for mutsig records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper mutSigFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {            
            BeanMap mutSigRecordBeanMap = BeanMap.create(new MutSigRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));

            // fill property values for bean using intersection of properties and expected mutsig columns
            Set<String> intersection = Sets.intersection(mutSigStagingDataMap.keySet(), fieldsetNames);
            intersection.stream().forEach((column) -> {
                mutSigRecordBeanMap.put(mutSigStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            MutSigRecord record = (MutSigRecord) mutSigRecordBeanMap.getBean();
            
            return record;
        };
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public MutSigRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!mutSigDataResults.isEmpty()) {
            return mutSigDataResults.remove(0);
        }
        return null;
    }
    
}

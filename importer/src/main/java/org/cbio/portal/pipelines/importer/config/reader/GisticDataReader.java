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
import org.cbio.portal.pipelines.importer.model.GisticRecord;

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
public class GisticDataReader implements ItemStreamReader<GisticRecord> {

    private final Map<String, String> gisticStagingDataMap = new GisticRecord().getGisticStagingDataMap();
    
    private CancerStudy cancerStudy;
    private List<GisticRecord> gisticRecordResults;
    
    private static final Log LOG = LogFactory.getLog(GisticDataReader.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        File dataFile = (File) executionContext.get("dataFile");
        MultiKeyMap gisticMetadata = (MultiKeyMap) executionContext.get("gisticMetadata");
        String[] header = (String[]) gisticMetadata.get(dataFile.getName(), "header");
        Integer numRecords = (Integer) gisticMetadata.get(dataFile.getName(), "numRecords");
        
        // load gistic data from datafile
        List<GisticRecord> gisticDataList = new ArrayList();
        try {
            LOG.info("Loading gistic data from: " + dataFile.getName());
            gisticDataList = loadGisticData(dataFile, header);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        
        // log whether data was loaded or not from gistic datafile
        if (gisticDataList.isEmpty()) {
            LOG.error("Error loading gistic data from: " + dataFile.getName());
        }
        else {
            LOG.info("Loaded " + gisticDataList.size() + "/" + numRecords + " from: " + dataFile.getName());
        }
        
        this.gisticRecordResults = gisticDataList;
    }
    
    /**
     * Loads gistic data from the datafile.
     * 
     * @param dataFile
     * @param gisticHeader
     * @return List<GisticRecord>
     */
    private List<GisticRecord> loadGisticData(File dataFile, String[] gisticHeader) throws Exception {
        // init tab-delim tokenizer with the gistic file header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(gisticHeader);
        
        // init line mapper for gistic file
        DefaultLineMapper<GisticRecord> lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(gisticFieldSetMapper());
        
        // set up gistic file reader context
        FlatFileItemReader<GisticRecord> gisticDataReader = new FlatFileItemReader();
        gisticDataReader.setResource(new FileSystemResource(dataFile));
        gisticDataReader.setLineMapper(lineMapper);
        gisticDataReader.setLinesToSkip(1);
        gisticDataReader.open(new ExecutionContext());
        
        List<GisticRecord> gisticDataList = new ArrayList();
        GisticRecord record = gisticDataReader.read();
        while (record != null) {
            // update gistic record with cancer study id
            record.setCancerStudyId(cancerStudy.getCancerStudyId());
            gisticDataList.add(record);
            
            record = gisticDataReader.read();
        }
        gisticDataReader.close();
        
        return gisticDataList;
    }
    
    /**
     * Field set mapper for gistic records.
     * 
     * @return FieldSetMapper
     */
    private FieldSetMapper gisticFieldSetMapper() {
        return (FieldSetMapper) (FieldSet fs) -> {
            BeanMap gisticRecordBeanMap = BeanMap.create(new GisticRecord());
            Set<String> fieldsetNames = new HashSet(Arrays.asList(fs.getNames()));

            // fill property values for bean using intersection of properties and expected gistic columns
            Set<String> intersection = Sets.intersection(gisticStagingDataMap.keySet(), fieldsetNames);
            intersection.stream().forEach((column) -> {
                gisticRecordBeanMap.put(gisticStagingDataMap.get(column), 
                        Strings.isNullOrEmpty(fs.readRawString(column))?null:fs.readRawString(column));
            });
            GisticRecord record = (GisticRecord) gisticRecordBeanMap.getBean();
            
            return record;
        };
    }    

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public GisticRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!gisticRecordResults.isEmpty()) {
            return gisticRecordResults.remove(0);
        }
        return null;
    }
    
}

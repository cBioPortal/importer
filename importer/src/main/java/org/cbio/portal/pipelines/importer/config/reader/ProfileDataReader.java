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
import org.cbio.portal.pipelines.importer.model.ProfileDataRecord;

import java.io.*;
import java.util.*;
import com.google.common.base.Strings;
import org.apache.commons.logging.*;
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
public class ProfileDataReader implements ItemStreamReader<ProfileDataRecord> {
    
    private final Map<String, String> profileNonCaseIdsMap = new ProfileDataRecord().getNonCaseIdsMap();
    
    private GeneticProfile geneticProfile;
    private int samplesSkipped;
    private boolean isRppaProfile;
    private boolean isCnaData;
    private List<ProfileDataRecord> compositeProfileDataResults;
    
    private final Set<Integer> caseIdSet = new LinkedHashSet<>();

    private static final Log LOG = LogFactory.getLog(ProfileDataReader.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.geneticProfile = (GeneticProfile) executionContext.get("geneticProfile");
        this.isRppaProfile = (boolean) executionContext.get("isRppaProfile");
        this.isCnaData = (boolean) executionContext.get("isCnaData");
        List<File> dataFileList = (List<File>) executionContext.get("dataFileList");
        MultiKeyMap profileMetadata = (MultiKeyMap) executionContext.get("profileMetadata");
                
        List<ProfileDataRecord> compositeProfileDataList = new ArrayList();
        dataFileList.stream().forEach((dataFile) -> {            
            // get profile metadata for current datafile
            String[] header = (String[]) profileMetadata.get(dataFile.getName(), "header");
            Set<String> nonCaseIds = (Set<String>) profileMetadata.get(dataFile.getName(), "nonCaseIds");
            HashMap<String, Integer> caseIdsMap = (HashMap<String, Integer>) profileMetadata.get(dataFile.getName(), "caseIdsMap");            
            Set<String> normalCaseIds = (Set<String>) profileMetadata.get(dataFile.getName(), "normalCaseIds");
            
            List<ProfileDataRecord> compositeProfileData = new ArrayList(); 
            try {
                LOG.info("Loading profile data from: " + dataFile.getName());
                compositeProfileData = loadDataFileProfileData(dataFile, header, nonCaseIds, caseIdsMap);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
            
            if (compositeProfileData.isEmpty()) {
                LOG.error("Error loading profile data from: " + dataFile.getName());
            }
            else {            
                // increment total sample count and number of normal samples skipped
                this.samplesSkipped += normalCaseIds.size();
                
                LOG.info("Loaded " + compositeProfileData.size() + " records from: " + dataFile.getName());
                compositeProfileDataList.addAll(compositeProfileData);
                
                // add sample stable ids to case id set
                caseIdSet.addAll(caseIdsMap.values());
            }
        });        
        // add samples skipped and case list to execution context for listener
        executionContext.put("samplesSkipped", samplesSkipped);
        executionContext.put("caseList", caseIdSet);
   
        this.compositeProfileDataResults = compositeProfileDataList;
    }

    /**
     * Loads profile data from the datafile.
     * 
     * @param dataFile
     * @param header
     * @param nonCaseIds
     * @param caseIdsMap
     * @return List<CompositeProfileData>
     * @throws Exception 
     */
    private List<ProfileDataRecord> loadDataFileProfileData(File dataFile, String[] header, Set<String> nonCaseIds, HashMap<String, Integer> caseIdsMap) throws Exception {        
        // init tab-delim tokenizer with the profile datafile header
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(header);
        
        // init line mapper for profile datafile
        DefaultLineMapper<ProfileDataRecord> lineMapper = new DefaultLineMapper<>();        
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(profileFieldSetMapper(nonCaseIds, caseIdsMap));
        
        // set up profile datafile reader context
        FlatFileItemReader<ProfileDataRecord> profileDataReader = new FlatFileItemReader();
        profileDataReader.setResource(new FileSystemResource(dataFile));
        profileDataReader.setLineMapper(lineMapper);
        profileDataReader.setLinesToSkip(1);
        profileDataReader.open(new ExecutionContext());

        List<ProfileDataRecord> compositeProfileDataList = new ArrayList();
        ProfileDataRecord record = profileDataReader.read();
        while (record != null) {
            // update record with rppa profile, cna data status, and genetic profile id
            record.setRppaProfile(isRppaProfile);
            record.setCnaData(isCnaData);
            record.setGeneticProfileId(geneticProfile.getGeneticProfileId());            
            compositeProfileDataList.add(record);
            
            record = profileDataReader.read();
        }
        profileDataReader.close();       
        
        return compositeProfileDataList;
    }

    /**
     * Field set mapper for profile data records.
     * 
     * @param nonCaseIds
     * @param caseIdsMap
     * @return FieldSetMapper
     */
    private FieldSetMapper profileFieldSetMapper(Set<String> nonCaseIds, Map<String, Integer> caseIdsMap) {
        return (FieldSetMapper) (FieldSet fs) -> {

            // generate linked hashmap of case data
            Map<Integer, String> caseProfileRecordData = new LinkedHashMap<>();
            caseIdsMap.keySet().stream().forEach((sampleStableId) -> {
                caseProfileRecordData.put(caseIdsMap.get(sampleStableId), fs.readRawString(sampleStableId));
            });

            // fill in values for non case id columns
            BeanMap profileRecordBeanMap = BeanMap.create(new ProfileDataRecord());
            for(String nonCaseIdCol : nonCaseIds){
                // ignore fields that are not in the profile record model
                if (!profileNonCaseIdsMap.containsKey(nonCaseIdCol.toUpperCase())) {
                    continue;
                }
                profileRecordBeanMap.put(profileNonCaseIdsMap.get(nonCaseIdCol.toUpperCase()), 
                        Strings.isNullOrEmpty(fs.readRawString(nonCaseIdCol))?null:fs.readRawString(nonCaseIdCol));
            }
            ProfileDataRecord record = (ProfileDataRecord) profileRecordBeanMap.getBean();
            record.setCaseProfileDataMap(caseProfileRecordData);
            
            return record;
        };
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public ProfileDataRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!compositeProfileDataResults.isEmpty()) {
            return compositeProfileDataResults.remove(0);
        }
        return null;
    }
    
}

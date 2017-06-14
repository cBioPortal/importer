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
import org.mskcc.cbio.persistence.jdbc.*;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;

import java.io.*;
import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;

import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.*;

/**
 *
 * @author ochoaa
 */
public class ProfileMetadataTasklet implements Tasklet {
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    @Autowired
    SampleProfileJdbcDaoImpl sampleProfileJdbcDaoImpl;
    
    @Autowired
    GeneticProfileSamplesJdbcDaoImpl geneticProfileSamplesJdbcDaoImpl;
    
    private GeneticProfile geneticProfile;
    private Integer genePanelId;
        
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        this.geneticProfile = (GeneticProfile) chunkContext.getStepContext().getJobExecutionContext().get("geneticProfile");
        this.genePanelId = (Integer) chunkContext.getStepContext().getJobExecutionContext().get("genePanelId");
        MultiKeyMap datatypeMetadata = (MultiKeyMap) chunkContext.getStepContext().getJobExecutionContext().get("datatypeMetadata");
        String datatype = (String) chunkContext.getStepContext().getJobExecutionContext().get("currentDatatype");
        List<File> dataFileList = (List<File>) datatypeMetadata.get(datatype, "dataFileList");
        
        // go through each datafile and load datafile metadata 
        MultiKeyMap profileMetadata = new MultiKeyMap();
        for (File dataFile : dataFileList) {
            MultiKeyMap metadata = loadProfileMetadata(dataFile);
            profileMetadata.putAll(metadata);
        }
        // add datafile list and metadata to execution context
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("dataFileList", dataFileList);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("profileMetadata", profileMetadata);        
        
        return RepeatStatus.FINISHED;
    }
    
    /**
     * Loads the profile metadata from the file (non case ids, map of case ids to internal sample ids, and normal case ids)
     * 
     * @param dataFile
     * @return MultiKeyMap
     */
    private MultiKeyMap loadProfileMetadata(File dataFile) throws IOException {
        MultiKeyMap profileMetadata = DataFileUtils.loadDataFileMetadata(dataFile);
        String[] header = (String[]) profileMetadata.get(dataFile.getName(), "header");
        
        // organize columns in header into non-case ids, case ids, and normal case ids
        Set<String> nonCaseIds = new HashSet<>();
        HashMap<String, Integer> caseIdsMap = new LinkedHashMap<>();
        Set<String> normalCaseIds = new HashSet<>();        
        for (String column : header) {
            // add non-case id columns to hash set
            if (DataFileUtils.nonCaseIdColumnNames.contains(column.toUpperCase())) {
                nonCaseIds.add(column);
            }
            else {
                String sampleStableId = DataFileUtils.getSampleStableId(column);
                // add normal case ids to normal case ids hash set
                if (DataFileUtils.isNormalSample(sampleStableId)) {
                    normalCaseIds.add(column);
                }
                // add non-normal case ids to linked hash map where key=stable id, val=internal sample id
                else {
                    Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, geneticProfile.getCancerStudyId());
                    if (sample == null) {
                        continue;
                    }
                    caseIdsMap.put(column, sample.getInternalId());
                    
                    // add sample profile for genetic profile (assumed that file has passed validation and no duplicate case ids in column exist)
                    sampleProfileJdbcDaoImpl.addSampleProfile(sample.getInternalId(), geneticProfile.getGeneticProfileId(), genePanelId);
                }
            }            
        }        
        // add samples to genetic profile samples
        geneticProfileSamplesJdbcDaoImpl.addGeneticProfileSamples(geneticProfile, new ArrayList(caseIdsMap.values()));
        profileMetadata.put(dataFile.getName(), "nonCaseIds", nonCaseIds);
        profileMetadata.put(dataFile.getName(), "caseIdsMap", caseIdsMap);
        profileMetadata.put(dataFile.getName(), "normalCaseIds", normalCaseIds);
        
        return profileMetadata;
    }
    
}

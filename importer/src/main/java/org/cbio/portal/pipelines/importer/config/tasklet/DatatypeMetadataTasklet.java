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

import org.mskcc.cbio.model.CancerStudy;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;

import java.io.*;
import java.util.*;
import javax.annotation.Resource;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class DatatypeMetadataTasklet implements Tasklet {
    
    @Value("#{jobParameters[stagingDirectory]}")
    private String stagingDirectory;
    
    @Resource(name="datatypeMetadataMap")
    MultiKeyMap datatypeMetadataMap;
    
    private static final Log LOG = LogFactory.getLog(DatatypeMetadataTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        CancerStudy cancerStudy = (CancerStudy) chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().get("cancerStudy");
        MultiKeyMap datatypeMetadata = new MultiKeyMap();
        
        // search cancer study path for all datatype metafiles by meta filenames
        LOG.info("Searching cancer study path for all datatype metafiles");
        Set<MultiKey> keys = datatypeMetadataMap.keySet();
        for(MultiKey key : keys){
            // init datatype metadata params
            String datatype = (String) key.getKey(0);
            boolean importData = false;
            Properties properties = new Properties();
            List<File> dataFileList = new ArrayList();
            List<String> logMessages = new ArrayList();
            
            // create metafile from datatype meta filename
            String metaFilename = (String) datatypeMetadataMap.get(datatype, "meta_filename");
            if (metaFilename.startsWith("<CANCER_STUDY>")) {
                metaFilename = metaFilename.replace("<CANCER_STUDY>", cancerStudy.getCancerStudyIdentifier());
            }
            File metaFile = new File(stagingDirectory, metaFilename);
            logMessages.add("Searching for meta filename: " + metaFile.getName());
            if (metaFile.exists()) {
                // load properties from metafile if exists
                properties.load(new FileInputStream(metaFile));
                
                // get data filename(s) from metafile and add to list of data files if exists
                String[] dataFilenames = DataFileUtils.splitDataFields(properties.getProperty("data_filename"));
                for (String dataFilename : dataFilenames) {
                    File dataFile = new File(stagingDirectory, dataFilename);
                    if (dataFile.exists()) {
                        dataFileList.add(dataFile);
                    }
                }
                
                if (!dataFileList.isEmpty()) {
                    // if datafile list is not empty then set import data status to true
                    logMessages.add("Found data files for datatype: " + datatype + " - beginning import");
                    importData = true;
                }
                else {
                    logMessages.add("Data files not found - skipping import for datatype: " + datatype);
                }
            }
            else {
                logMessages.add("Meta file not found - skipping import for datatype: " + datatype);
            }
            // add datatype metadata to multikeymap for execution context
            datatypeMetadata.put(datatype, "importData", importData);
            datatypeMetadata.put(datatype, "properties", properties);
            datatypeMetadata.put(datatype, "dataFileList", dataFileList);
            datatypeMetadata.put(datatype, "logMessages", logMessages);
            datatypeMetadata.put(datatype, "caseList", new LinkedHashSet<>()); // for adding case ids 
        }
        
        // add datatype metadata to execution context
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("datatypeMetadata", datatypeMetadata);
        return RepeatStatus.FINISHED;
    }

}

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
import org.apache.commons.logging.*;
import org.apache.commons.collections.map.MultiKeyMap;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class CopyNumberSegmentMetadataTasklet implements Tasklet {
    
    @Autowired
    CopyNumberSegmentJdbcDaoImpl copyNumberSegmentJdbcDaoImpl;
    
    @Autowired
    CancerStudyJdbcDaoImpl cancerStudyJdbcDaoImpl;
    
    private CancerStudy cancerStudy;
    
    private static final Log LOG = LogFactory.getLog(CopyNumberSegmentMetadataTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        // get cancer study, datatype, and metafile properties from datatype metadata in execution context
        this.cancerStudy = (CancerStudy) chunkContext.getStepContext().getJobExecutionContext().get("cancerStudy");
        MultiKeyMap datatypeMetadata = (MultiKeyMap) chunkContext.getStepContext().getJobExecutionContext().get("datatypeMetadata");
        String datatype = (String) chunkContext.getStepContext().getJobExecutionContext().get("currentDatatype");
        Properties properties = (Properties) datatypeMetadata.get(datatype, "properties");
        List<File> dataFileList = (List<File>) datatypeMetadata.get(datatype, "dataFileList");
        File dataFile = dataFileList.get(0);

        // import copy number segment file and load metadata from file
        LOG.info("Importing COPY_NUMBER_SEG_FILE record with data filename: " + dataFile.getName());
        CopyNumberSegmentFile copyNumberSegmentFile = loadCopyNumberSegmentFile(properties);
        copyNumberSegmentJdbcDaoImpl.addCopyNumberSegmentFile(copyNumberSegmentFile);
        MultiKeyMap copyNumberSegmentMetadata = DataFileUtils.loadDataFileMetadata(dataFile);

        // add datafile and copy number segment metadata to execution context
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("dataFile", dataFile);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("copyNumberSegmentMetadata", copyNumberSegmentMetadata);
        
        return RepeatStatus.FINISHED;
    }
    
    /**
     * Loads an instance of CopyNumberSegmentFile from the copy number segment metafile properties.
     * 
     * @param properties
     * @return CopyNumberSegmentFile
     */
    private CopyNumberSegmentFile loadCopyNumberSegmentFile(Properties properties) {
        // create new copy number seg file object and set fields
        CopyNumberSegmentFile copyNumberSegmentFile = new CopyNumberSegmentFile();       
        copyNumberSegmentFile.setCancerStudyId(cancerStudy.getCancerStudyId());
        
        // set reference genome id        
        CopyNumberSegmentFile.ReferenceGenomeId referenceGenomeId = CopyNumberSegmentFile.ReferenceGenomeId.valueOf(properties.getProperty("reference_genome_id"));
        copyNumberSegmentFile.setReferenceGenomeId(referenceGenomeId);
        
        // set remaining copy number seg file properties
        copyNumberSegmentFile.setDescription(properties.getProperty("description"));
        copyNumberSegmentFile.setFilename(properties.getProperty("data_filename"));
        
        return copyNumberSegmentFile;
    }
    
}

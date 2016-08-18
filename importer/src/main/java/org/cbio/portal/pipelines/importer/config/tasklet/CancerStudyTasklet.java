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
import org.mskcc.cbio.persistence.jdbc.CancerStudyJdbcDaoImpl;

import java.io.*;
import java.util.*;
import org.apache.commons.logging.*;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.*;

/**
 * Tasklet to import cancer study from meta_study.txt
 * @author ochoaa
 */
@JobScope
public class CancerStudyTasklet implements Tasklet {

    @Value("#{jobParameters[stagingDirectory]}")
    private String stagingDirectory;

    @Autowired
    CancerStudyJdbcDaoImpl cancerStudyJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(CancerStudyTasklet.class);
    
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        File metaFile = new File(stagingDirectory, "meta_study.txt");
        
        // try to load cancer study meta data from study path
        CancerStudy cancerStudy = loadCancerStudy(metaFile);
        if (cancerStudy == null) {
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("importCancerStudy", false);
            return RepeatStatus.FINISHED;
        }
        
        // check for existing study with matching cancer study identifier and delete if exists
        String cancerStudyIdentifier = cancerStudy.getCancerStudyIdentifier();
        CancerStudy existingStudy = cancerStudyJdbcDaoImpl.getCancerStudy(cancerStudy.getCancerStudyIdentifier());
        if (existingStudy != null) {
            LOG.warn("Cancer study found with matching cancer study id: " + existingStudy.getCancerStudyIdentifier());
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("existingStudy", existingStudy);
            cancerStudyJdbcDaoImpl.deleteCancerStudy(existingStudy.getCancerStudyId());
        }
        // import new cancer study
        LOG.info("Importing cancer study: " + cancerStudyIdentifier);
        CancerStudy newCancerStudy = cancerStudyJdbcDaoImpl.addCancerStudy(cancerStudy);
        
        // add default rollback state, original cancer study identifier, cancer study, and import cancer study status to execution context
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("rollbackCancerStudyState", false);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("cancerStudy", newCancerStudy);
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("importCancerStudy", true);
        return RepeatStatus.FINISHED;
    }

    /**
     * Load cancer study from meta_study.txt.
     * Returns null if meta file doesn't exist or error loading meta data
     * 
     * @param metaFilename
     * @return CancerStudy
     */
    private CancerStudy loadCancerStudy(File metaFile) throws IOException {
        CancerStudy newCancerStudy = null;
        
        if (!metaFile.exists()) {
            LOG.error("Could not find meta_study.txt in study path: " + stagingDirectory);
        }
        else {
            Properties properties = new Properties();
            properties.load(new FileInputStream(metaFile));

            try {
                newCancerStudy = loadMetaStudyProperties(properties);
            }
            catch (NullPointerException ex) {
                LOG.error("Error loading: " + metaFile.getCanonicalPath());
            }
        }
        
        return newCancerStudy;
    }
    
    /**
     * Load cancer study from meta_study.txt properties.
     * 
     * @param properties
     * @return CancerStudy
     */
    private CancerStudy loadMetaStudyProperties(Properties properties) {
        CancerStudy cancerStudy = new CancerStudy();
        cancerStudy.setCancerStudyId(-1);
        cancerStudy.setCancerStudyIdentifier(properties.getProperty("cancer_study_identifier"));
        cancerStudy.setTypeOfCancerId(properties.getProperty("type_of_cancer"));
        cancerStudy.setName(properties.getProperty("name"));
        cancerStudy.setDescription(properties.getProperty("description"));
        cancerStudy.setImportDate(new Date());
        
        String shortName = cancerStudy.getName();
        boolean publicStudy = false;
        String pmid = "";
        String citation = "";
        String groups = "";
        int status = 0;        
        try {
            shortName = properties.getProperty("short_name");
            publicStudy = properties.getProperty("public_study").equalsIgnoreCase("true");
            
            pmid = properties.getProperty("pmid");
            citation = properties.getProperty("citation");
            groups = properties.getProperty("groups");
            status = properties.getProperty("status").equals("1")?1:0;
        }
        catch (NullPointerException ex) {}
        
        cancerStudy.setShortName(shortName);
        cancerStudy.setPublicStudy(publicStudy);
        cancerStudy.setPmid(pmid);
        cancerStudy.setCitation(citation);
        cancerStudy.setGroups(groups);
        cancerStudy.setStatus(status);
        
        return cancerStudy;
    }

}

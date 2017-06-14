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

package org.cbio.portal.pipelines.importer.config;

import org.mskcc.cbio.persistence.jdbc.InfoJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.config.listener.BatchStudyImporterListener;

import javax.annotation.Resource;
import org.apache.commons.logging.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
@ComponentScan("org.mskcc.cbio.persistence.jdbc")
public class BatchConfiguration {
    
    public static final String BATCH_STUDY_IMPORTER_JOB = "batchStudyImporterJob";
    public static final String DELETE_CANCER_STUDY_JOB = "deleteCancerStudyJob";
    
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Value("${db.version}")
    private String dbVersion;
    
    @Resource(name="importCancerStudy")
    public Step importCancerStudy;
    
    @Resource(name="deleteCancerStudy")
    public Step deleteCancerStudy;
    
    @Autowired
    InfoJdbcDaoImpl infoJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(BatchConfiguration.class);   
    
    /**
     * Job for importing a cancer study.
     * 
     * @return Job
     * @throws Exception 
     */
    @Bean
    public Job batchStudyImporterJob() throws Exception {
        return jobBuilderFactory.get(BATCH_STUDY_IMPORTER_JOB)
                .incrementer(new RunIdIncrementer())
                .listener(batchStudyImporterListener())
                .start(batchStudyImporterFlow())
                .build()
                .build();
    }
    
    /**
     * Job for deleting a cancer study given a cancer study identifier. 
     * 
     * @return Job
     * @throws Exception 
     */
    @Bean
    public Job deleteCancerStudyJob() throws Exception {
        return jobBuilderFactory.get(DELETE_CANCER_STUDY_JOB)
                .start(deleteCancerStudy)
                .build();
    }
    
    /**
     * Flow for initiating cancer study batch import.
     * 
     * @return Flow
     */
    @Bean
    public Flow batchStudyImporterFlow() {
        return new FlowBuilder<Flow>("batchStudyImporterFlow")
                .start(batchStudyImporterDecider())
                    .on("STOPPED").end()
                .from(batchStudyImporterDecider())
                    .on("CONTINUE")
                    .to(importCancerStudy)
                .build();
    }
    
    /**
     * Listener for batch study importer job execution.
     * 
     * @return JobExecutionListener
     */
    @Bean
    public JobExecutionListener batchStudyImporterListener() {
        return new BatchStudyImporterListener();
    }
    
    /**
     * Decider for checking DB schema compatibility. 
     * If DB schema is not compatible then job will stop executing.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider batchStudyImporterDecider() {        
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            LOG.info("Checking DB schema compatibility");
            if (!infoJdbcDaoImpl.checkPortalDbVersion()) {
                LOG.error("DB version expected by portal: " + dbVersion + 
                        ". DB schema version found: " + infoJdbcDaoImpl.getDbSchemaVersion());
                return FlowExecutionStatus.STOPPED;
            }
            else {
                LOG.info("DB schema version matches version expected by portal");
                return new FlowExecutionStatus("CONTINUE");
            }
        };
    }
    
}

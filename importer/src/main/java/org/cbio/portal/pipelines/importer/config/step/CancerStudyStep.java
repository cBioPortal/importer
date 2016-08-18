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

package org.cbio.portal.pipelines.importer.config.step;

import org.cbio.portal.pipelines.importer.config.tasklet.*;
import org.cbio.portal.pipelines.importer.config.listener.CancerStudyListener;

import javax.annotation.Resource;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.*;
import org.springframework.batch.core.step.tasklet.Tasklet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;

/**
 * Configuration for importing a cancer study. 
 * 
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
public class CancerStudyStep {
    
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Resource(name="importClinicalData")
    public Step importClinicalData;
    
    @Resource(name="importTimelineData")
    public Step importTimelineData;
    
    @Resource(name="importMutSigData")
    public Step importMutSigData;
    
    @Resource(name="importCopyNumberSegData")
    public Step importCopyNumberSegData;
    
    @Resource(name="importGisticGenesData")
    public Step importGisticGenesData;
        
    @Resource(name="importProteinLevelData")
    public Step importProteinLevelData;
    
    @Resource(name="importCnaData")
    public Step importCnaData;
    
    @Resource(name="importGeneExpressionData")
    public Step importGeneExpressionData;
    
    @Resource(name="importMethylationData")
    public Step importMethylationData;

    @Resource(name="importMutationData")
    public Step importMutationData;
    
    @Resource(name="importFusionData")
    public Step importFusionData;
    
    @Resource(name="importStructuralVariantData")
    public Step importStructuralVariantData;
    
    @Resource(name="importCaseLists")
    public Step importCaseLists;
    
    /**
     * Step for initiating cancer study import.
     * 
     * @return Step
     */
    @Bean
    public Step importCancerStudy() {
        return stepBuilderFactory.get("importCancerStudy")
                .flow(importCancerStudyFlow())
                .build();
    }    
    
    @Bean
    public Step deleteCancerStudy() {
        return stepBuilderFactory.get("deleteCancerStudy")
                .tasklet(deleteCancerStudyTasklet())
                .build();
    }    
    
    /**
     * Flow for importing a cancer study.
     * 
     * @return Flow
     */    
    @Bean
    public Flow importCancerStudyFlow() { 
        return new FlowBuilder<Flow>("importCancerStudyFlow")
                .start(importCancerStudyStep())
                .next(importCancerStudyDataDecider())
                    .on("STOPPED").stop()
                .from(importCancerStudyDataDecider())
                    .on("CONTINUE")
                    .to(loadCancerStudyDatatypeMetadata())
                    .next(importClinicalData)
                    .next(continueImportingStudyDataDecider())
                        .on("STOPPED").stop()
                    .from(continueImportingStudyDataDecider())
                        .on("CONTINUE")
                        .to(importTimelineData)
                        .next(importMutSigData)
                        .next(importCopyNumberSegData)
                        .next(importGisticGenesData)
                        .next(importProteinLevelData)
                        .next(importCnaData)
                        .next(importGeneExpressionData)
                        .next(importMethylationData)
                        .next(importMutationData)
                        .next(importFusionData)
                        .next(importStructuralVariantData)
                        .next(importCaseLists)
                .build();
    }

    /**
     * Import cancer study step and tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step importCancerStudyStep() {
        return stepBuilderFactory.get("importCancerStudyStep")
                .listener(cancerStudyListener())
                .tasklet(cancerStudyTasklet())                
                .build();
    }    

    /**
     * Tasklet for loading cancer study meta data.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet cancerStudyTasklet() {
        return new CancerStudyTasklet();
    }    
    
    /**
     * Listener for cancer study import.
     * 
     * @return StepExecutionListener
     */
    @Bean
    public StepExecutionListener cancerStudyListener() {
        return new CancerStudyListener();
    }
    
    /**
     * Step for implementing datatype metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadCancerStudyDatatypeMetadata() {
        return stepBuilderFactory.get("loadCancerStudyDatatypeMetdata")
                .tasklet(datatypeMetadataTasklet())
                .build();
    }
    
    @Bean
    @StepScope
    public Tasklet deleteCancerStudyTasklet() {
        return new DeleteCancerStudyTasklet();
    }
    
    /**
     * Tasklet for loading datatype metadata. 
     * Searches cancer study path for meta files and determines whether importer
     * should import each datatype depending on whether metafile and datafiles exist
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet datatypeMetadataTasklet() {
        return new DatatypeMetadataTasklet();
    }

    /**
     * Decider to determine if cancer study was loaded properly.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider importCancerStudyDataDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            if ((boolean) jobExecution.getExecutionContext().get("importCancerStudy")) {
                return new FlowExecutionStatus("CONTINUE");
            }
            else {            
                return FlowExecutionStatus.STOPPED;
            }
        };
    }
    
    /**
     * Decider that checks rollback status for cancer study after the clinical data step runs.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider continueImportingStudyDataDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            if ((boolean) jobExecution.getExecutionContext().get("rollbackCancerStudyState")) {
                return FlowExecutionStatus.FAILED;
            }
            else {
                return new FlowExecutionStatus("CONTINUE");
            }
        };
    }

}

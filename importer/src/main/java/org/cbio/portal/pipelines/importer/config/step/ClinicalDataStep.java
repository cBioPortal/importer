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

import org.cbio.portal.pipelines.importer.config.composite.CompositeClinicalData;
import org.cbio.portal.pipelines.importer.config.tasklet.ClinicalAttributeTasklet;
import org.cbio.portal.pipelines.importer.config.listener.ClinicalDataListener;
import org.cbio.portal.pipelines.importer.config.reader.ClinicalDataReader;
import org.cbio.portal.pipelines.importer.config.processor.ClinicalDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.ClinicalDataWriter;

import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.*;
import org.springframework.batch.repeat.RepeatStatus;

import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
public class ClinicalDataStep {
    
    public static final String IMPORT_CLINICAL_DATA = "importClinicalData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(ClinicalDataStep.class);
    
    /**
     * Step for importing clinical data implemented by cancer study work flow.
     * 
     * @return Step
     */
    @Bean
    public Step importClinicalData() {
        return stepBuilderFactory.get(IMPORT_CLINICAL_DATA)
                .flow(clinicalStepFlow())
                .build();
    }    
    
    /**
     * Clinical data flow.
     * Execution of clinical data steps will begin with "clinicalStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow clinicalStepFlow() {
        // execute clinical data steps
        return new FlowBuilder<Flow>("clinicalStepFlow")
                .start(initClinicalImportStep())
                .next(clinicalStep())
                .build();
    }

    /**
     * Step to initiate flow for clinical data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initClinicalImportStep() {
        return stepBuilderFactory.get("initClinicalImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
        }).build();
    }
    
    /***************************************************************************
     * Clinical data import steps and flows.
     **************************************************************************/
        
    /**
     * Step for implementing clinical attribute tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadClinicalAttributes() {
        return stepBuilderFactory.get("loadClinicalAttributes")
                .tasklet(clinicalAttributeTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading clinical attributes from clinical data files.
     * 
     * @return Tasklet
     */
    @Bean    
    @StepScope
    public Tasklet clinicalAttributeTasklet() {
        return new ClinicalAttributeTasklet();
    }
    
    /**
     * Flow for importing mixed clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow clinicalStep() {
        return new FlowBuilder<Flow>("clinicalStep")
                .start(clinicalStepDecider())
                    .on("RUN").to(loadClinicalAttributes())
                    .next(clinicalStepBuilder("clinicalStep"))
                    .next(clinicalPatientStep())
                .from(clinicalStepDecider())
                    .on("SKIP").to(clinicalPatientStep())
                .build();
    }
    
    /**
     * Flow for importing patient clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow clinicalPatientStep() {
        return new FlowBuilder<Flow>("clinicalPatientStep")
                .start(clinicalPatientStepDecider())
                    .on("RUN").to(loadClinicalAttributes())
                    .next(clinicalStepBuilder("clinicalPatientStep"))
                    .next(clinicalSampleStep())
                .from(clinicalPatientStepDecider())
                    .on("SKIP").to(clinicalSampleStep())
                .build();
    }

    /**
     * Flow for importing sample clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow clinicalSampleStep() {
        return new FlowBuilder<Flow>("clinicalSampleStep")
                .start(clinicalSampleStepDecider())
                    .on("RUN").to(loadClinicalAttributes())                
                    .next(clinicalStepBuilder("clinicalSampleStep"))
                    .next(bcrClinicalStep())
                .from(clinicalSampleStepDecider())
                    .on("SKIP").to(bcrClinicalStep())
                .build();        
    }    
    
    /**
     * Flow for importing BCR mixed clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow bcrClinicalStep() {
        return new FlowBuilder<Flow>("bcrClinicalStep")
                .start(bcrClinicalStepDecider())
                    .on("RUN").to(loadClinicalAttributes())
                    .next(clinicalStepBuilder("bcrClinicalStep"))
                    .next(bcrClinicalPatientStep())
                .from(bcrClinicalStepDecider())
                    .on("SKIP").to(bcrClinicalPatientStep())
                .build();
    }
    
    /**
     * Flow for importing BCR patient clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow bcrClinicalPatientStep() {
        return new FlowBuilder<Flow>("bcrClinicalPatientStep")
                .start(bcrClinicalPatientStepDecider())
                    .on("RUN").to(loadClinicalAttributes())
                    .next(clinicalStepBuilder("bcrClinicalPatientStep"))
                    .next(bcrClinicalSampleStep())
                .from(bcrClinicalPatientStepDecider())
                    .on("SKIP").to(bcrClinicalSampleStep())
                .build();
    }
    
    /**
     * Flow for importing BCR sample clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow bcrClinicalSampleStep() {
        return new FlowBuilder<Flow>("bcrClinicalSampleStep")
                .start(bcrClinicalSampleStepDecider())
                    .on("RUN").to(loadClinicalAttributes())                
                    .next(clinicalStepBuilder("bcrClinicalSampleStep"))
                    .next(clinicalSuppStep())
                .from(bcrClinicalSampleStepDecider())
                    .on("SKIP").to(clinicalSuppStep())
                .build();        
    }
    
    /**
     * Flow for importing supplemental clinical data.
     * 
     * @return Flow
     */
    @Bean
    public Flow clinicalSuppStep() {
        return new FlowBuilder<Flow>("clinicalSuppStep")
                .start(clinicalSuppStepDecider())
                    .on("RUN").to(loadClinicalAttributes())
                    .next(clinicalStepBuilder("clinicalSuppStep"))
                .from(clinicalSuppStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for clinical data.
     * 
     * @param stepName
     * @return Step
     */
    public Step clinicalStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<CompositeClinicalData, CompositeClinicalData> chunk(chunkInterval)
                .reader(clinicalDataReader())
                .processor(clinicalDataProcessor())
                .writer(clinicalDataWriter())
                .listener(clinicalDataListener())
                .build();
    }
    
    /***************************************************************************
     * Clinical data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener clinicalDataListener() {
        return new ClinicalDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<CompositeClinicalData> clinicalDataReader() {
        return new ClinicalDataReader();
    }
    
    @Bean
    public ClinicalDataProcessor clinicalDataProcessor() {
        return new ClinicalDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<CompositeClinicalData> clinicalDataWriter() {
        return new ClinicalDataWriter();
    }    

    /***************************************************************************
     * Deciders for clinical data steps.
     **************************************************************************/
    
    /**
     * Clinical step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider clinicalStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {            
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "clinical";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
    /**
     * Clinical patient step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider clinicalPatientStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "clinical-patient";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }

    /**
     * Clinical sample step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider clinicalSampleStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "clinical-sample";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
    /**
     * BCR clinical step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider bcrClinicalStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "bcr-clinical";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
    /**
     * BCR clinical patient step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider bcrClinicalPatientStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "bcr-clinical-patient";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }

    /**
     * BCR clinical sample step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider bcrClinicalSampleStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "bcr-clinical-sample";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
    /**
     * Clinical supplemental step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider clinicalSuppStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "clinical-supp";
            
            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            }
            else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
}

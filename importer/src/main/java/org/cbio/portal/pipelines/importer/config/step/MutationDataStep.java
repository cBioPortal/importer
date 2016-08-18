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

import org.cbio.portal.pipelines.importer.model.MafRecord;
import org.cbio.portal.pipelines.importer.config.composite.CompositeMutationData;
import org.cbio.portal.pipelines.importer.config.tasklet.*;
import org.cbio.portal.pipelines.importer.config.listener.MutationDataListener;
import org.cbio.portal.pipelines.importer.config.reader.MutationDataReader;
import org.cbio.portal.pipelines.importer.config.processor.MutationDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.MutationDataWriter;

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
public class MutationDataStep {
    
    public static final String IMPORT_MUTATION_DATA = "importMutationData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    private static final Log LOG = LogFactory.getLog(MutationDataStep.class);
    
    @Bean
    public Step importMutationData() {
        return stepBuilderFactory.get(IMPORT_MUTATION_DATA)
                .flow(mutationStepFlow())
                .build();
    }
    
    /**
     * Mutation data flow. 
     * Execution of mutation data steps will begin with "mutationStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow mutationStepFlow() {
        // execute mutation data steps sequentially starting with mutationStep
        return new FlowBuilder<Flow>("mutationStepFlow")
                .start(initMutationImportStep())
                .next(mutationStep())
                .build();
    }
    
    /**
     * Step to initiate flow for mutation data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initMutationImportStep() {
        return stepBuilderFactory.get("initMutationImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /**************************************************************************
     * Mutation data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadMutationGeneticProfile() {
        return stepBuilderFactory.get("loadMutationGeneticProfile")
                .tasklet(mutationGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet mutationGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing mutation metadata tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadMutationMetadata() {
        return stepBuilderFactory.get("loadMutationMetadata")
                .tasklet(loadMutationMetadataTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading metadata from MAF files. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadMutationMetadataTasklet() {
        return new MutationMetadataTasklet();
    }
    
    /**
     * Flow for importing mutation data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow mutationStep() {
        return new FlowBuilder<Flow>("mutationStep")
                .start(mutationStepDecider())
                    .on("RUN").to(loadMutationGeneticProfile())
                    .next(loadMutationMetadata())
                    .next(mutationStepBuilder("mutationStep"))
                    .next(mutationGermlineStep())
                .from(mutationStepDecider())
                    .on("SKIP").to(mutationGermlineStep())
                .build();
    }
    
    /**
     * Flow for importing germline mutation data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow mutationGermlineStep() {
        return new FlowBuilder<Flow>("mutationGermlineStep")
                .start(mutationGermlineStepDecider())
                    .on("RUN").to(loadMutationGeneticProfile())
                    .next(loadMutationMetadata())
                    .next(mutationStepBuilder("mutationGermlineStep"))
                    .next(mutationManualStep())
                .from(mutationGermlineStepDecider())
                .on("SKIP").to(mutationManualStep())
                .build();
    }
    
    /**
     * Flow for importing manually curated mutation data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow mutationManualStep() {
        return new FlowBuilder<Flow>("mutationManualStep")
                .start(mutationManualStepDecider())
                    .on("RUN").to(loadMutationGeneticProfile())
                    .next(loadMutationMetadata())
                    .next(mutationStepBuilder("mutationManualStep"))
                .from(mutationManualStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for mutation data.
     * 
     * @param stepName
     * @return Step
     */
    public Step mutationStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<MafRecord, CompositeMutationData> chunk(chunkInterval)
                .reader(mutationDataReader())
                .processor(mutationDataProcessor())
                .writer(mutationDataWriter())
                .listener(mutationDataListener())
                .build();
    }
    
    /***************************************************************************
     * Mutation data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener mutationDataListener() {
        return new MutationDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<MafRecord> mutationDataReader() {
        return new MutationDataReader();
    }
    
    @Bean
    public MutationDataProcessor mutationDataProcessor() {
        return new MutationDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<CompositeMutationData> mutationDataWriter() {
        return new MutationDataWriter();
    }
    
    /*******************************************************************
     * Deciders for mutation data steps.
     *******************************************************************/
    
    /**
     * Mutation step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider mutationStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mutation";
            
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
     * Mutation Germline step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider mutationGermlineStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mutation-germline";
            
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
     * Mutation Manual step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider mutationManualStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mutation-manual";
            
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

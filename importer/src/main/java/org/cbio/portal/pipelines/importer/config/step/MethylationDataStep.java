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
import org.cbio.portal.pipelines.importer.model.ProfileDataRecord;
import org.cbio.portal.pipelines.importer.config.listener.ProfileDataListener;
import org.cbio.portal.pipelines.importer.config.reader.ProfileDataReader;
import org.cbio.portal.pipelines.importer.config.processor.ProfileDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.ProfileDataWriter;

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
public class MethylationDataStep {
     
    public static final String IMPORT_METHYLATION_DATA = "importMethylationData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(MethylationDataStep.class);
    
    @Bean
    public Step importMethylationData() {
        return stepBuilderFactory.get(IMPORT_METHYLATION_DATA)
                .flow(methylationStepFlow())
                .build();
    }
    
    /**
     * Methylation data flow. 
     * Execution of methylation data steps will begin with "methylationHm27Step" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow methylationStepFlow() {
        // execute methylation data steps sequentially starting with methylationHm27Step
        return new FlowBuilder<Flow>("methylationStepFlow")
                .start(initMethylationImportStep())
                .next(methylationHm27Step())
                .build();
    }
    
    /**
     * Step to initiate flow for methylation data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initMethylationImportStep() {
        return stepBuilderFactory.get("initMethylationImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /***************************************************************************
     * Methylation data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadMethylationGeneticProfile() {
        return stepBuilderFactory.get("loadMethylationGeneticProfile")
                .tasklet(methylationGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet methylationGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing profile metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadMethylationMetadata() {
        return stepBuilderFactory.get("loadMethylationMetadata")
                .tasklet(loadMethylationMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading profile metadata from profile datafiles.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadMethylationMetadataTasklet() {
        return new ProfileMetadataTasklet();
    }
    
    /**
     * Flow for importing methylation-hm27 data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow methylationHm27Step() {
        return new FlowBuilder<Flow>("methylationHm27Step")
                .start(methylationHm27StepDecider())
                    .on("RUN").to(loadMethylationGeneticProfile())
                    .next(loadMethylationMetadata())
                    .next(methylationStepBuilder("methylationHm27Step"))
                    .next(methylationHm450Step())
                .from(methylationHm27StepDecider())
                    .on("SKIP").to(methylationHm450Step())
                .build();
    }
    
    /**
     * Flow for importing methylation-hm450 data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow methylationHm450Step() {
        return new FlowBuilder<Flow>("methylationHm450Step")
                .start(methylationHm450StepDecider())
                    .on("RUN").to(loadMethylationGeneticProfile())
                    .next(loadMethylationMetadata())
                    .next(methylationStepBuilder("methylationHm450Step"))
                .from(methylationHm450StepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for methylation data.
     * 
     * @param stepName
     * @return Step
     */
    public Step methylationStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<ProfileDataRecord, ProfileDataRecord> chunk(chunkInterval)
                .reader(methylationDataReader())
                .processor(methylationDataProcessor())
                .writer(methylationDataWriter())
                .listener(methylationDataListener())
                .build();
    }
    
    /***************************************************************************
     * Methylation data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener methylationDataListener() {
        return new ProfileDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<ProfileDataRecord> methylationDataReader() {
        return new ProfileDataReader();
    }
    
    @Bean
    public ProfileDataProcessor methylationDataProcessor() {
        return new ProfileDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<ProfileDataRecord> methylationDataWriter() {
        return new ProfileDataWriter();
    }
    
    /***************************************************************************
     * Deciders for methylation data steps.
     **************************************************************************/
    
    /**
     * Methylation-hm27 step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider methylationHm27StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "methylation-hm27";
            
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
     * Methylation-hm450 step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider methylationHm450StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "methylation-hm450";
            
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

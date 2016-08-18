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
public class ProteinLevelDataStep {
     
    public static final String IMPORT_PROTEIN_LEVEL_DATA = "importProteinLevelData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(ProteinLevelDataStep.class);   
    
    @Bean
    public Step importProteinLevelData() {
        return stepBuilderFactory.get(IMPORT_PROTEIN_LEVEL_DATA)
                .flow(proteinLevelStepFlow())
                .build();
    }
    
    /**
     * Protein-level data flow. 
     * Execution of protein-level data steps will begin with "rppaStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow proteinLevelStepFlow() {
        // execute protein-level data steps sequentially starting with rppaStep
        return new FlowBuilder<Flow>("proteinLevelStepFlow")
                .start(initProteinLevelImportStep())
                .next(rppaStep())
                .build();
    }
    
    /**
     * Step to initiate flow for protein-level data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initProteinLevelImportStep() {
        return stepBuilderFactory.get("initProteinLevelImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /***************************************************************************
     * Protein-level data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadProteinLevelGeneticProfile() {
        return stepBuilderFactory.get("loadProteinLevelGeneticProfile")
                .tasklet(proteinLevelGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet proteinLevelGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing profile metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadProteinLevelMetadata() {
        return stepBuilderFactory.get("loadProteinLevelMetadata")
                .tasklet(loadProteinLevelMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading profile metadata from profile datafiles.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadProteinLevelMetadataTasklet() {
        return new ProfileMetadataTasklet();
    }

    /**
     * Flow for importing RPPA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow rppaStep() {
        return new FlowBuilder<Flow>("rppaStep")
                .start(rppaStepDecider())
                    .on("RUN").to(loadProteinLevelGeneticProfile())
                    .next(loadProteinLevelMetadata())
                    .next(proteinLevelStepBuilder("rppaStep"))
                    .next(rppaZscoresStep())
                .from(rppaStepDecider())
                    .on("SKIP").to(rppaZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing RPPA Z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow rppaZscoresStep() {
        return new FlowBuilder<Flow>("rppaZscoresStep")
                .start(rppaZscoresStepDecider())
                    .on("RUN").to(loadProteinLevelGeneticProfile())
                    .next(loadProteinLevelMetadata())
                    .next(proteinLevelStepBuilder("rppaZscoresStep"))
                    .next(proteinQuantificationStep())
                .from(rppaZscoresStepDecider())
                    .on("SKIP").to(proteinQuantificationStep())
                .build();
    }
    
    /**
     * Flow for importing protein quantification data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow proteinQuantificationStep() {
        return new FlowBuilder<Flow>("proteinQuantificationStep")
                .start(proteinQuantificationStepDecider())
                    .on("RUN").to(loadProteinLevelGeneticProfile())
                    .next(loadProteinLevelMetadata())
                    .next(proteinLevelStepBuilder("proteinQuantificationStep"))
                .from(proteinQuantificationStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for protein-level data.
     * 
     * @param stepName
     * @return Step
     */
    public Step proteinLevelStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<ProfileDataRecord, ProfileDataRecord> chunk(chunkInterval)
                .reader(proteinLevelDataReader())
                .processor(proteinLevelDataProcessor())
                .writer(proteinLevelDataWriter())
                .listener(proteinLevelDataListener())
                .build();
    }
    
    /***************************************************************************
     * Protein-level data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener proteinLevelDataListener() {
        return new ProfileDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<ProfileDataRecord> proteinLevelDataReader() {
        return new ProfileDataReader();
    }
    
    @Bean
    public ProfileDataProcessor proteinLevelDataProcessor() {
        return new ProfileDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<ProfileDataRecord> proteinLevelDataWriter() {
        return new ProfileDataWriter();
    }
    
    /***************************************************************************
     * Deciders for protein-level data steps.
     **************************************************************************/
    
    /**
     * RPPA step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider rppaStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rppa";
            
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
     * RPPA Z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider rppaZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rppa-zscores";
            
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
     * Protein quantification step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider proteinQuantificationStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "protein-quantification";
            
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

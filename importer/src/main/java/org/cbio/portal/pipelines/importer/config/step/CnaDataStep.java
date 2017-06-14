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

import org.cbio.portal.pipelines.importer.model.ProfileDataRecord;
import org.cbio.portal.pipelines.importer.config.listener.ProfileDataListener;
import org.cbio.portal.pipelines.importer.config.tasklet.GeneticProfileTasklet;
import org.cbio.portal.pipelines.importer.config.tasklet.ProfileMetadataTasklet;
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
public class CnaDataStep {
    
    public static final String IMPORT_CNA_DATA = "importCnaData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(CnaDataStep.class);
    
    @Bean
    public Step importCnaData() {
        return stepBuilderFactory.get(IMPORT_CNA_DATA)
                .flow(cnaStepFlow())
                .build();
    }
    
    /**
     * CNA data flow. 
     * Execution of CNA data steps will begin with "cnaStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow cnaStepFlow() {
        // execute CNA data steps sequentially starting with cnaStep
        return new FlowBuilder<Flow>("cnaStepFlow")
                .start(initCnaImportStep())
                .next(cnaStep())
                .build();
    }
    
    /**
     * Step to initiate flow for CNA data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initCnaImportStep() {
        return stepBuilderFactory.get("initCnaImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /***************************************************************************
     * CNA data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadCnaGeneticProfile() {
        return stepBuilderFactory.get("loadCnaGeneticProfile")
                .tasklet(cnaGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet cnaGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing profile metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadCnaMetadata() {
        return stepBuilderFactory.get("loadCnaMetadata")
                .tasklet(loadCnaMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading profile metadata from profile datafiles.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadCnaMetadataTasklet() {
        return new ProfileMetadataTasklet();
    }

    /**
     * Flow for importing CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaStep() {
        return new FlowBuilder<Flow>("cnaStep")
                .start(cnaStepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaStep"))
                    .next(cnaFoundationStep())
                .from(cnaStepDecider())
                    .on("SKIP").to(cnaFoundationStep())
                .build();
    }
    
    /**
     * Flow for importing Foundation CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaFoundationStep() {
        return new FlowBuilder<Flow>("cnaFoundationStep")
                .start(cnaFoundationStepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaFoundationStep"))
                    .next(cnaRaeStep())
                .from(cnaFoundationStepDecider())
                    .on("SKIP").to(cnaRaeStep())
                .build();
    }
    
    /**
     * Flow for importing RAE CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaRaeStep() {
        return new FlowBuilder<Flow>("cnaRaeStep")
                .start(cnaRaeStepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaRaeStep"))
                    .next(cnaConsensusStep())
                .from(cnaRaeStepDecider())
                    .on("SKIP").to(cnaConsensusStep())
                .build();
    }
    
    /**
     * Flow for importing consensus CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaConsensusStep() {
        return new FlowBuilder<Flow>("cnaConsensusStep")
                .start(cnaConsensusStepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaConsensusStep"))
                    .next(cnaLinearStep())
                .from(cnaConsensusStepDecider())
                    .on("SKIP").to(cnaLinearStep())
                .build();
    }
    
     /**
     * Flow for importing linear CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaLinearStep() {
        return new FlowBuilder<Flow>("cnaLinearStep")
                .start(cnaLinearStepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaLinearStep"))
                    .next(cnaLog2Step())
                .from(cnaLinearStepDecider())
                    .on("SKIP").to(cnaLog2Step())
                .build();
    }
    
    /**
     * Flow for importing log2 CNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow cnaLog2Step() {
        return new FlowBuilder<Flow>("cnaLog2Step")
                .start(cnaLog2StepDecider())
                    .on("RUN").to(loadCnaGeneticProfile())
                    .next(loadCnaMetadata())
                    .next(cnaStepBuilder("cnaLog2Step"))
                .from(cnaLog2StepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for CNA data.
     * 
     * @param stepName
     * @return Step
     */
    public Step cnaStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<ProfileDataRecord, ProfileDataRecord> chunk(chunkInterval)
                .reader(cnaDataReader())
                .processor(cnaDataProcessor())
                .writer(cnaDataWriter())
                .listener(cnaDataListener())
                .build();
    }

    /***************************************************************************
     * CNA data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener cnaDataListener() {
        return new ProfileDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<ProfileDataRecord> cnaDataReader() {
        return new ProfileDataReader();
    }
    
    @Bean
    public ProfileDataProcessor cnaDataProcessor() {
        return new ProfileDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<ProfileDataRecord> cnaDataWriter() {
        return new ProfileDataWriter();
    }

    /***************************************************************************
     * Deciders for CNA data steps.
     **************************************************************************/
    
    /**
     * CNA step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-gistic";
            
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
     * CNA Foundation step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaFoundationStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-foundation";
            
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
     * CNA RAE step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaRaeStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-rae";
            
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
     * CNA consensus step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaConsensusStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-consensus";
            
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
     * CNA linear step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaLinearStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "linear-cna-gistic";
            
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
     * CNA log2 step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider cnaLog2StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "log2-cna";
            
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

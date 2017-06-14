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

import org.cbio.portal.pipelines.importer.model.FusionRecord;
import org.cbio.portal.pipelines.importer.config.composite.CompositeMutationData;
import org.cbio.portal.pipelines.importer.config.tasklet.*;
import org.cbio.portal.pipelines.importer.config.listener.MutationDataListener;
import org.cbio.portal.pipelines.importer.config.reader.FusionDataReader;
import org.cbio.portal.pipelines.importer.config.processor.FusionDataProcessor;
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
public class FusionDataStep {
    
    public static final String IMPORT_FUSION_DATA = "importFusionData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    private static final Log LOG = LogFactory.getLog(FusionDataStep.class);
    
    @Bean
    public Step importFusionData() {
        return stepBuilderFactory.get(IMPORT_FUSION_DATA)
                .flow(fusionStepFlow())
                .build();
    }
    
    /**
     * Fusion data import flow.
     * 
     * @return Flow
     */
    @Bean
    public Flow fusionStepFlow() {
        // execute fusion data import flow
        return new FlowBuilder<Flow>("importFusionStepFlow")
                .start(initFusionImportStep())
                .next(fusionStepDecider())
                    .on("RUN").to(loadFusionGeneticProfile())
                    .next(loadFusionMetadata())
                    .next(fusionStep())
                .from(fusionStepDecider())
                    .on("SKIP").end()
                .build();
    }
  
    /**
     * Step to initiate flow for fusion data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initFusionImportStep() {
        return stepBuilderFactory.get("initFusionImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /**************************************************************************
     * Fusion data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadFusionGeneticProfile() {
        return stepBuilderFactory.get("loadFusionGeneticProfile")
                .tasklet(fusionGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet fusionGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing mutation metadata tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadFusionMetadata() {
        return stepBuilderFactory.get("loadFusionMetadata")
                .tasklet(loadFusionMetadataTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading metadata from fusion files. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadFusionMetadataTasklet() {
        return new MutationMetadataTasklet();
    }

    /**
     * Step for importing fusion data.
     * 
     * @return Step
     */
    public Step fusionStep() {
        return stepBuilderFactory.get("fusionStep")
                .<FusionRecord, CompositeMutationData> chunk(chunkInterval)
                .reader(fusionDataReader())
                .processor(fusionDataProcessor())
                .writer(fusionDataWriter())
                .listener(fusionDataListener())
                .build();
    }
    
    /***************************************************************************
     * Fusion data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener fusionDataListener() {
        return new MutationDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<FusionRecord> fusionDataReader() {
        return new FusionDataReader();
    }
    
    @Bean
    public FusionDataProcessor fusionDataProcessor() {
        return new FusionDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<CompositeMutationData> fusionDataWriter() {
        return new MutationDataWriter();
    }
    
    /*******************************************************************
     * Decider for fusion data step.
     *******************************************************************/
    
    /**
     * Fusion step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider fusionStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "fusion";
            
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

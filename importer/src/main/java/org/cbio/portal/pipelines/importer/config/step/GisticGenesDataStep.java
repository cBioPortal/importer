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

import org.mskcc.cbio.model.Gistic;
import org.cbio.portal.pipelines.importer.model.GisticRecord;
import org.cbio.portal.pipelines.importer.config.tasklet.GisticMetadataTasklet;
import org.cbio.portal.pipelines.importer.config.listener.GisticDataListener;
import org.cbio.portal.pipelines.importer.config.reader.GisticDataReader;
import org.cbio.portal.pipelines.importer.config.processor.GisticDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.GisticDataWriter;

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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
public class GisticGenesDataStep {
    
    public static final String IMPORT_GISTIC_GENES_DATA = "importGisticGenesData";

    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(GisticGenesDataStep.class);
    
    @Bean
    public Step importGisticGenesData() {
        return stepBuilderFactory.get(IMPORT_GISTIC_GENES_DATA)
                .flow(gisticGenesStepFlow())
                .build();
    }
        
    /**
     * Gistic data flow. 
     * Execution of gistic data steps will begin with "gisticGenesAmpStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow gisticGenesStepFlow() {
        return new FlowBuilder<Flow>("gisticGenesStepFlow")
                .start(initGisticGenesImportStep())
                .next(gisticGenesAmpStep())
                .build();
    }
    
    /**
     * Step to initiate flow for gistic data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initGisticGenesImportStep() {
        return stepBuilderFactory.get("initGisticGenesImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /***************************************************************************
     * Gistic data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing gistic metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadGisticMetadata() {
        return stepBuilderFactory.get("loadGisticMetadata")
                .tasklet(gisticMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading gistic metadata.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet gisticMetadataTasklet() {
        return new GisticMetadataTasklet();
    }
    
    /**
     * Gistic amp data import flow.
     * 
     * @return Flow
     */
    @Bean
    public Flow gisticGenesAmpStep() {
        return new FlowBuilder<Flow>("gisticGenesAmpStep")
                .start(gisticGenesAmpStepDecider())
                    .on("RUN").to(loadGisticMetadata())
                    .next(gisticStepBuilder("gisticGenesAmpStep"))
                    .next(gisticGenesDelStep())
                .from(gisticGenesAmpStepDecider())
                    .on("SKIP").to(gisticGenesDelStep())
                .build();
    }
    
    /**
     * Gistic del data import flow.
     * 
     * @return Flow
     */
    @Bean
    public Flow gisticGenesDelStep() {
        return new FlowBuilder<Flow>("gisticGenesDelStep")
                .start(gisticGenesDelStepDecider())
                    .on("RUN").to(loadGisticMetadata())
                    .next(gisticStepBuilder("gisticGenesDelStep"))
                .from(gisticGenesDelStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for gistic data.
     * 
     * @param stepName
     * @return Step
     */
    public Step gisticStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<GisticRecord, Gistic> chunk(chunkInterval)
                .reader(gisticDataReader())
                .processor(gisticDataProcessor())
                .writer(gisticDataWriter())
                .listener(gisticDataListener())
                .build();
    }
    
    /***************************************************************************
     * Gistic data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener gisticDataListener() {
        return new GisticDataListener();
    }

    @Bean
    @StepScope    
    public ItemStreamReader<GisticRecord> gisticDataReader() {
        return new GisticDataReader();
    }
    
    @Bean
    public GisticDataProcessor gisticDataProcessor() {
        return new GisticDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<Gistic> gisticDataWriter() {
        return new GisticDataWriter();
    }
    
    /***************************************************************************
     * Deciders for gistic data step.
     **************************************************************************/    
    
    /**
     * Gistic amp step decider.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider gisticGenesAmpStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "gistic-genes-amp";
            
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
     * Gistic del step decider.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider gisticGenesDelStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "gistic-genes-del";
            
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

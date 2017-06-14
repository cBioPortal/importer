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

import org.mskcc.cbio.model.CopyNumberSegment;
import org.cbio.portal.pipelines.importer.model.CopyNumberSegmentRecord;
import org.cbio.portal.pipelines.importer.config.tasklet.CopyNumberSegmentMetadataTasklet;
import org.cbio.portal.pipelines.importer.config.listener.CopyNumberSegmentDataListener;
import org.cbio.portal.pipelines.importer.config.reader.CopyNumberSegmentDataReader;
import org.cbio.portal.pipelines.importer.config.processor.CopyNumberSegmentDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.CopyNumberSegmentDataWriter;

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
public class CopyNumberSegmentDataStep {
    
    public static final String IMPORT_COPY_NUMBER_SEG_DATA = "importCopyNumberSegData";

    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(CopyNumberSegmentDataStep.class);
    
    @Bean
    public Step importCopyNumberSegData() {
        return stepBuilderFactory.get(IMPORT_COPY_NUMBER_SEG_DATA)
                .flow(copyNumberSegStepFlow())
                .build();
    }
        
    /**
     * Copy number segment data flow. 
     * Execution of copy number segment data steps will begin with "copyNumberSegmentHg19DataStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow copyNumberSegStepFlow() {
        return new FlowBuilder<Flow>("copyNumberSegStepFlow")
                .start(initCopyNumberSegmentImportStep())
                .next(copyNumberSegmentHg19DataStep())
                .build();
    }
    
    /**
     * Step to initiate flow for copy number segment data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initCopyNumberSegmentImportStep() {
        return stepBuilderFactory.get("initCopyNumberSegmentImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }
    
    /***************************************************************************
     * Copy number segment data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing copy number segment metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadCopyNumberSegmentMetadata() {
        return stepBuilderFactory.get("loadCopyNumberSegmentMetadata")
                .tasklet(copyNumberSegmentMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading copy number segment metadata.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet copyNumberSegmentMetadataTasklet() {
        return new CopyNumberSegmentMetadataTasklet();
    }
    
    /**
     * Copy number segment data import flow for reference genome id hg19.
     * 
     * @return Flow
     */
    @Bean
    public Flow copyNumberSegmentHg19DataStep() {
        return new FlowBuilder<Flow>("copyNumberSegmentHg19DataStep")
                .start(copyNumberSegmentHg19StepDecider())
                    .on("RUN").to(loadCopyNumberSegmentMetadata())
                    .next(copyNumberSegmentStepBuilder("copyNumberSegmentHg19DataStep"))
                    .next(copyNumberSegmentHg18DataStep())
                .from(copyNumberSegmentHg19StepDecider())
                    .on("SKIP").to(copyNumberSegmentHg18DataStep())
                .build();
    }
    
    /**
     * Copy number segment data import flow for reference genome id hg18.
     * 
     * @return Flow
     */
    @Bean
    public Flow copyNumberSegmentHg18DataStep() {
        return new FlowBuilder<Flow>("copyNumberSegmentHg18DataStep")
                .start(copyNumberSegmentHg18StepDecider())
                    .on("RUN").to(loadCopyNumberSegmentMetadata())
                    .next(copyNumberSegmentStepBuilder("copyNumberSegmentHg18DataStep"))
                .from(copyNumberSegmentHg18StepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for copy number segment data.
     * 
     * @param stepName
     * @return Step
     */
    public Step copyNumberSegmentStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<CopyNumberSegmentRecord, CopyNumberSegment> chunk(chunkInterval)
                .reader(copyNumberSegmentDataReader())
                .processor(copyNumberSegmentDataProcessor())
                .writer(copyNumberSegmentDataWriter())
                .listener(copyNumberSegmentDataListener())
                .build();
    }
    
    /***************************************************************************
     * Copy number segment data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener copyNumberSegmentDataListener() {
        return new CopyNumberSegmentDataListener();
    }

    @Bean
    @StepScope    
    public ItemStreamReader<CopyNumberSegmentRecord> copyNumberSegmentDataReader() {
        return new CopyNumberSegmentDataReader();
    }
    
    @Bean
    public CopyNumberSegmentDataProcessor copyNumberSegmentDataProcessor() {
        return new CopyNumberSegmentDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<CopyNumberSegment> copyNumberSegmentDataWriter() {
        return new CopyNumberSegmentDataWriter();
    }
    
    /***************************************************************************
     * Deciders for copy number segment data step.
     **************************************************************************/    
    
    /**
     * Copy number segment step decider for reference genome id hg19.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider copyNumberSegmentHg19StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-hg19-seg";
            
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
     * Copy number segment step decider for reference genome id hg18.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider copyNumberSegmentHg18StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "cna-hg18-seg";
            
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

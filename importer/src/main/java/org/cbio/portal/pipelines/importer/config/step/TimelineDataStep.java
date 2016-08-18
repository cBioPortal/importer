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

import org.mskcc.cbio.model.ClinicalEvent;
import org.cbio.portal.pipelines.importer.model.TimelineRecord;
import org.cbio.portal.pipelines.importer.config.tasklet.TimelineMetadataTasklet;
import org.cbio.portal.pipelines.importer.config.listener.TimelineDataListener;
import org.cbio.portal.pipelines.importer.config.reader.TimelineDataReader;
import org.cbio.portal.pipelines.importer.config.processor.TimelineDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.TimelineDataWriter;

import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.*;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
public class TimelineDataStep {
    
    public static final String IMPORT_TIMELINE_DATA = "importTimelineData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(TimelineDataStep.class);
    
    @Bean
    public Step importTimelineData() {
        return stepBuilderFactory.get(IMPORT_TIMELINE_DATA)
                .flow(timelineStepFlow())
                .build();
    }
    
    /***************************************************************************
     * Timeline data import steps and flows.
     **************************************************************************/
    
    /**
     * Timeline data flow. 
     * Execution of timeline data import is dependent on whether the timeline metafile and 
     * datafiles exist or not. 
     * If not then timeline data import is skipped
     * 
     * @return Flow
     */
    @Bean
    public Flow timelineStepFlow() {
        return new FlowBuilder<Flow>("timelineStepFlow")
                .start(timelineStepDecider())
                    .on("RUN").to(loadTimelineMetadata())
                    .next(timelineStep())
                .from(timelineStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Step for implementing timeline metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadTimelineMetadata() {
        return stepBuilderFactory.get("loadTimelineMetadata")
                .tasklet(timelineMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading timeline metadata.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet timelineMetadataTasklet() {
        return new TimelineMetadataTasklet();
    }

    /**
     * Timeline data import step.
     * 
     * @return Step
     */
    @Bean Step timelineStep() {
        return stepBuilderFactory.get("timelineStep")
                .<TimelineRecord, ClinicalEvent> chunk(chunkInterval)
                .reader(timelineDataReader())
                .processor(timelineDataProcessor())
                .writer(timelineDataWriter())
                .listener(timelineDataListener())
                .build();
    }
    
    /***************************************************************************
     * Timeline data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener timelineDataListener() {
        return new TimelineDataListener();
    }

    @Bean
    @StepScope    
    public ItemStreamReader<TimelineRecord> timelineDataReader() {
        return new TimelineDataReader();
    }
    
    @Bean
    public TimelineDataProcessor timelineDataProcessor() {
        return new TimelineDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<ClinicalEvent> timelineDataWriter() {
        return new TimelineDataWriter();
    }
    
    /***************************************************************************
     * Deciders for timeline data step.
     **************************************************************************/
    
    /**
     * Timeline step decider.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider timelineStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "time-line-data";
            
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

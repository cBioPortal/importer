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

import org.mskcc.cbio.model.MutSig;
import org.cbio.portal.pipelines.importer.model.MutSigRecord;
import org.cbio.portal.pipelines.importer.config.tasklet.MutSigMetadataTasklet;
import org.cbio.portal.pipelines.importer.config.listener.MutSigDataListener;
import org.cbio.portal.pipelines.importer.config.reader.MutSigDataReader;
import org.cbio.portal.pipelines.importer.config.processor.MutSigDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.MutSigDataWriter;

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
public class MutSigDataStep {
    
    public static final String IMPORT_MUT_SIG_DATA = "importMutSigData";

    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(MutSigDataStep.class);
    
    @Bean
    public Step importMutSigData() {
        return stepBuilderFactory.get(IMPORT_MUT_SIG_DATA)
                .flow(mutSigStepFlow())
                .build();
    }
    
    /***************************************************************************
     * Mutsig data import steps and flows.
     **************************************************************************/
    
    /**
     * Mutsig data flow. 
     * Execution of mutsig data import is dependent on whether the mutsig metafile and 
     * datafile exist or not. 
     * If not then mutsig data import is skipped
     * 
     * @return Flow
     */
    @Bean
    public Flow mutSigStepFlow() {
        return new FlowBuilder<Flow>("mutSigStepFlow")
                .start(mutSigStepDecider())
                    .on("RUN").to(loadMutSigMetadata())
                    .next(mutSigStep())
                .from(mutSigStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Step for implementing mutsig metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadMutSigMetadata() {
        return stepBuilderFactory.get("loadMutSigMetadata")
                .tasklet(mutSigMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading mutsig metadata.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet mutSigMetadataTasklet() {
        return new MutSigMetadataTasklet();
    }
    
    /**
     * Mutsig data import step.
     * 
     * @return Step
     */
    @Bean 
    public Step mutSigStep() {
        return stepBuilderFactory.get("mutSigStep")
                .<MutSigRecord, MutSig> chunk(chunkInterval)
                .reader(mutSigDataReader())
                .processor(mutSigDataProcessor())
                .writer(mutSigDataWriter())
                .listener(mutSigDataListener())
                .build();
    }
    
    /***************************************************************************
     * Mutsig data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener mutSigDataListener() {
        return new MutSigDataListener();
    }

    @Bean
    @StepScope    
    public ItemStreamReader<MutSigRecord> mutSigDataReader() {
        return new MutSigDataReader();
    }
    
    @Bean
    public MutSigDataProcessor mutSigDataProcessor() {
        return new MutSigDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<MutSig> mutSigDataWriter() {
        return new MutSigDataWriter();
    }
    
    /***************************************************************************
     * Deciders for mutsig data step.
     **************************************************************************/
    
    /**
     * Mutsig step decider.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider mutSigStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mutation-significance-v2";
            
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

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

import org.mskcc.cbio.model.StructuralVariant;
import org.cbio.portal.pipelines.importer.model.StructuralVariantRecord;
import org.cbio.portal.pipelines.importer.config.tasklet.*;
import org.cbio.portal.pipelines.importer.config.listener.StructuralVariantDataListener;
import org.cbio.portal.pipelines.importer.config.reader.StructuralVariantDataReader;
import org.cbio.portal.pipelines.importer.config.processor.StructuralVariantDataProcessor;
import org.cbio.portal.pipelines.importer.config.writer.StructuralVariantDataWriter;

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
public class StructuralVariantDataStep {
    
    public static final String IMPORT_STRUCTURAL_VARIANT_DATA = "importStructuralVariantDataStep";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(StructuralVariantDataStep.class);
    
    @Bean
    public Step importStructuralVariantData() {
        return stepBuilderFactory.get(IMPORT_STRUCTURAL_VARIANT_DATA)
                .flow(structuralVariantStepFlow())
                .build();
    }
    
    /**************************************************************************
     * Mutation data import steps and flows.
     **************************************************************************/
    
    /**
     * Structural variant data flow.
     * Execution of structural variant data import is dependent on whether the 
     * structural variant metafile and datafile exist or not. 
     * If not then the structural variant data import is skipped
     * 
     * @return Flow
     */
    @Bean
    public Flow structuralVariantStepFlow() {
        return new FlowBuilder<Flow>("structuralVariantStepFlow")
                .start(structuralVariantStepDecider())
                    .on("RUN").to(loadStructuralVariantGeneticProfile())
                    .next(loadStructuralVariantMetadata())
                    .next(structuralVariantStep())
                .from(structuralVariantStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadStructuralVariantGeneticProfile() {
        return stepBuilderFactory.get("loadStructuralVariantGeneticProfile")
                .tasklet(structuralVariantGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet structuralVariantGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing structural variant metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadStructuralVariantMetadata() {
        return stepBuilderFactory.get("loadStructuralVariantMetadata")
                .tasklet(structuralVariantMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading structural variant metadata.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet structuralVariantMetadataTasklet() {
        return new StructuralVariantMetadataTasklet();
    }
    
    /** 
     * Structural variant data import step.
     * 
     * @return Step
     */
    @Bean
    public Step structuralVariantStep() {
        return stepBuilderFactory.get("structuralVariantStep")
                .<StructuralVariantRecord, StructuralVariant> chunk(chunkInterval)
                .reader(structuralVariantDataReader())
                .processor(structuralVariantDataProcessor())
                .writer(structuralVariantDataWriter())
                .listener(structuralVariantDataListener())
                .build();
    }
    
    /***************************************************************************
     * Structural variant data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener structuralVariantDataListener() {
        return new StructuralVariantDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<StructuralVariantRecord> structuralVariantDataReader() {
        return new StructuralVariantDataReader();
    }
    
    @Bean
    public StructuralVariantDataProcessor structuralVariantDataProcessor() {
        return new StructuralVariantDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<StructuralVariant> structuralVariantDataWriter() {
        return new StructuralVariantDataWriter();
    }
    
    /***************************************************************************
     * Decider for structural variant data step.
     **************************************************************************/    
    
    /**
     * Structural variant step decider.
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider structuralVariantStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "structural-variant";
            
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

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
public class GeneExpressionDataStep {
    
    public static final String IMPORT_GENE_EXPRESSION_DATA = "importGeneExpressionData";
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    private static final Log LOG = LogFactory.getLog(GeneExpressionDataStep.class);

    @Bean
    public Step importGeneExpressionData() {
        return stepBuilderFactory.get(IMPORT_GENE_EXPRESSION_DATA)
                .flow(geneExpressionStepFlow())
                .build();
    }
        
    /**
     * Gene expression data flow. 
     * Execution of gene expression data steps will begin with "geneExpressionAffymetrixStep" and will
     * execute conditionally based on their respective deciders
     * 
     * @return Flow
     */
    @Bean
    public Flow geneExpressionStepFlow() {
        // execute gene expression data steps sequentially starting with geneExpressionAffymetrixStep
        return new FlowBuilder<Flow>("geneExpressionStepFlow")
                .start(initGeneExpressionImportStep())
                .next(geneExpressionAffymetrixStep())
                .build();
    }
    
    /**
     * Step to initiate flow for gene expression data steps.
     * 
     * @return Step
     */
    @Bean
    public Step initGeneExpressionImportStep() {
        return stepBuilderFactory.get("initGeneExpressionImportStep")
                .tasklet((StepContribution sc, ChunkContext cc) -> {
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /***************************************************************************
     * Gene expression data import steps and flows.
     **************************************************************************/
    
    /**
     * Step for implementing genetic profile tasklet. 
     * 
     * @return Step
     */
    @Bean
    public Step loadGeneExpressionGeneticProfile() {
        return stepBuilderFactory.get("loadGeneExpressionGeneticProfile")
                .tasklet(geneExpressionGeneticProfileTasklet())
                .build();                
    }
    
    /**
     * Tasklet for loading and importing genetic profile from meta file. 
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet geneExpressionGeneticProfileTasklet() {
        return new GeneticProfileTasklet();
    }
    
    /**
     * Step for implementing profile metadata tasklet.
     * 
     * @return Step
     */
    @Bean
    public Step loadGeneExpressionMetadata() {
        return stepBuilderFactory.get("loadGeneExpressionMetadata")
                .tasklet(loadGeneExpressionMetadataTasklet())
                .build();
    }
    
    /**
     * Tasklet for loading profile metadata from profile datafiles.
     * 
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet loadGeneExpressionMetadataTasklet() {
        return new ProfileMetadataTasklet();
    }
    
    /**
     * Flow for importing gene expression Affymetrix data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionAffymetrixStep() {
        return new FlowBuilder<Flow>("geneExpressionAffymetrixStep")
                .start(geneExpressionAffymetrixStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionAffymetrixStep"))
                    .next(geneExpressionAffymetrixZscoresStep())
                .from(geneExpressionAffymetrixStepDecider())
                    .on("SKIP").to(geneExpressionAffymetrixZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression Affymetrix z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionAffymetrixZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionAffymetrixZscoresStep")
                .start(geneExpressionAffymetrixZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionAffymetrixZscoresStep"))
                    .next(geneExpressionMergedStep())
                .from(geneExpressionAffymetrixZscoresStepDecider())
                    .on("SKIP").to(geneExpressionMergedStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression merged data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMergedStep() {
        return new FlowBuilder<Flow>("geneExpressionMergedStep")
                .start(geneExpressionMergedStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMergedStep"))
                    .next(geneExpressionMergedZscoresStep())
                .from(geneExpressionMergedStepDecider())
                    .on("SKIP").to(geneExpressionMergedZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression merged z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMergedZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionMergedZscoresStep")
                .start(geneExpressionMergedZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMergedZscoresStep"))
                    .next(geneExpressionRnaSeqStep())
                .from(geneExpressionMergedZscoresStepDecider())
                    .on("SKIP").to(geneExpressionRnaSeqStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression RNAseq data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionRnaSeqStep() {
        return new FlowBuilder<Flow>("geneExpressionRnaSeqStep")
                .start(geneExpressionRnaSeqStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionRnaSeqStep"))
                    .next(geneExpressionRnaSeqZscoresStep())
                .from(geneExpressionRnaSeqStepDecider())
                    .on("SKIP").to(geneExpressionRnaSeqZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression RNAseq z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionRnaSeqZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionRnaSeqZscoresStep")
                .start(geneExpressionRnaSeqZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionRnaSeqZscoresStep"))
                    .next(geneExpressionAgilentStep())
                .from(geneExpressionRnaSeqZscoresStepDecider())
                    .on("SKIP").to(geneExpressionAgilentStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression Agilent data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionAgilentStep() {
        return new FlowBuilder<Flow>("geneExpressionAgilentStep")
                .start(geneExpressionAgilentStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionAgilentStep"))
                    .next(geneExpressionAgilentZscoresStep())
                .from(geneExpressionAgilentStepDecider())
                    .on("SKIP").to(geneExpressionAgilentZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression Agilent z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionAgilentZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionAgilentZscoresStep")
                .start(geneExpressionAgilentZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionAgilentZscoresStep"))
                    .next(geneExpressionRnaSeqV2Step())
                .from(geneExpressionAgilentZscoresStepDecider())
                    .on("SKIP").to(geneExpressionRnaSeqV2Step())
                .build();
    }
    
    /**
     * Flow for importing gene expression RNAseq-V2 data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionRnaSeqV2Step() {
        return new FlowBuilder<Flow>("geneExpressionRnaSeqV2Step")
                .start(geneExpressionRnaSeqV2StepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionRnaSeqV2Step"))
                    .next(geneExpressionRnaSeqV2ZscoresStep())
                .from(geneExpressionRnaSeqV2StepDecider())
                    .on("SKIP").to(geneExpressionRnaSeqV2ZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression RNAseq-V2 z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionRnaSeqV2ZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionRnaSeqV2ZscoresStep")
                .start(geneExpressionRnaSeqV2ZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionRnaSeqV2ZscoresStep"))
                    .next(geneExpressionMiRnaStep())
                .from(geneExpressionRnaSeqV2ZscoresStepDecider())
                    .on("SKIP").to(geneExpressionMiRnaStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression miRNA data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMiRnaStep() {
        return new FlowBuilder<Flow>("geneExpressionMiRnaStep")
                .start(geneExpressionMiRnaStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMiRnaStep"))
                    .next(geneExpressionMiRnaMedianZscoresStep())
                .from(geneExpressionMiRnaStepDecider())
                    .on("SKIP").to(geneExpressionMiRnaMedianZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression miRNA median z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMiRnaMedianZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionMiRnaMedianZscoresStep")
                .start(geneExpressionMiRnaMedianZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMiRnaMedianZscoresStep"))
                    .next(geneExpressionMiRnaMergedMedianZscoresStep())
                .from(geneExpressionMiRnaMedianZscoresStepDecider())
                    .on("SKIP").to(geneExpressionMiRnaMergedMedianZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression miRNA merged median z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMiRnaMergedMedianZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionMiRnaMergedMedianZscoresStep")
                .start(geneExpressionMiRnaMergedMedianZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMiRnaMergedMedianZscoresStep"))
                    .next(geneExpressionMRnaOutliersStep())
                .from(geneExpressionMiRnaMergedMedianZscoresStepDecider())
                    .on("SKIP").to(geneExpressionMRnaOutliersStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression mRNA outliers data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMRnaOutliersStep() {
        return new FlowBuilder<Flow>("geneExpressionMRnaOutliersStep")
                .start(geneExpressionMRnaOutliersStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMRnaOutliersStep"))
                    .next(geneExpressionCaptureStep())
                .from(geneExpressionMRnaOutliersStepDecider())
                    .on("SKIP").to(geneExpressionCaptureStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression capture data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionCaptureStep() {
        return new FlowBuilder<Flow>("geneExpressionCaptureStep")
                .start(geneExpressionCaptureStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionCaptureStep"))
                    .next(geneExpressionCaptureZscoresStep())
                .from(geneExpressionCaptureStepDecider())
                    .on("SKIP").to(geneExpressionCaptureZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression capture z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionCaptureZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionCaptureZscoresStep")
                .start(geneExpressionCaptureZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionCaptureZscoresStep"))
                    .next(geneExpressionOtherZscoresStep())
                .from(geneExpressionCaptureZscoresStepDecider())
                    .on("SKIP").to(geneExpressionOtherZscoresStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression capture z-scores data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionOtherZscoresStep() {
        return new FlowBuilder<Flow>("geneExpressionOtherZscoresStep")
                .start(geneExpressionOtherZscoresStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionOtherZscoresStep"))
                    .next(geneExpressionMRnaSeqFpkmStep())
                .from(geneExpressionOtherZscoresStepDecider())
                    .on("SKIP").to(geneExpressionMRnaSeqFpkmStep())
                .build();
    }
    
    /**
     * Flow for importing gene expression mRNAseq FPKM data.
     * 
     * @return Flow
     */
    @Bean 
    public Flow geneExpressionMRnaSeqFpkmStep() {
        return new FlowBuilder<Flow>("geneExpressionMRnaSeqFpkmStep")
                .start(geneExpressionMRnaSeqFpkmStepDecider())
                    .on("RUN").to(loadGeneExpressionGeneticProfile())
                    .next(loadGeneExpressionMetadata())
                    .next(geneExpressionStepBuilder("geneExpressionMRnaSeqFpkmStep"))
                .from(geneExpressionMRnaSeqFpkmStepDecider())
                    .on("SKIP").end()
                .build();
    }
    
    /**
     * Universal step builder for gene expression data.
     * 
     * @param stepName
     * @return Step
     */
    public Step geneExpressionStepBuilder(String stepName) {
        return stepBuilderFactory.get(stepName)
                .<ProfileDataRecord, ProfileDataRecord> chunk(chunkInterval)
                .reader(geneExpressionDataReader())
                .processor(geneExpressionDataProcessor())
                .writer(geneExpressionDataWriter())
                .listener(geneExpressionDataListener())
                .build();
    }
    
    /***************************************************************************
     * Gene Expression data listener, reader, processor, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener geneExpressionDataListener() {
        return new ProfileDataListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<ProfileDataRecord> geneExpressionDataReader() {
        return new ProfileDataReader();
    }
    
    @Bean
    public ProfileDataProcessor geneExpressionDataProcessor() {
        return new ProfileDataProcessor();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<ProfileDataRecord> geneExpressionDataWriter() {
        return new ProfileDataWriter();
    }
    
    /***************************************************************************
     * Deciders for gene expression data steps.
     **************************************************************************/
    
    /**
     * Gene expression Affymetrix step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionAffymetrixStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "affymetrix-gene-expression";
            
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
     * Gene expression Affymetrix z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionAffymetrixZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "affymetrix-gene-expression-zscores";
            
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
     * Gene expression merged step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMergedStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "gene-expression-merged";

            List<String> logMessages = (List<String>) datatypeMetadata.get(datatype, "logMessages");
            LOG.info(logMessages.get(0));
            if ((boolean) datatypeMetadata.get(datatype, "importData")) {
                LOG.info(logMessages.get(1));
                jobExecution.getExecutionContext().put("currentDatatype", datatype);
                return new FlowExecutionStatus("RUN");
            } else {
                LOG.warn(logMessages.get(1));
                return new FlowExecutionStatus("SKIP");
            }
        };
    }
    
    /**
     * Gene expression merged z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMergedZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "gene-expression-merged-zscores";
            
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
     * Gene expression RNAseq step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionRnaSeqStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rnaseq-gene-expression";
            
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
     * Gene expression RNAseq z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionRnaSeqZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rnaseq-gene-expression-zscores";
            
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
     * Gene expression Agilent step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionAgilentStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "agilent-gene-expression";
            
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
     * Gene expression Agilent z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionAgilentZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "agilent-gene-expression-zscores";
            
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
     * Gene expression RNAseq-V2 step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionRnaSeqV2StepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rnaseq-v2-gene-expression";
            
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
     * Gene expression RNAseq-V2 z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionRnaSeqV2ZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "rnaseq-v2-gene-expression-zscores";
            
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
     * Gene expression miRNA step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMiRnaStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mirna-expression";
            
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
     * Gene expression miRNA median z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMiRnaMedianZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mirna-median-zscores";
            
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
     * Gene expression miRNA merged median z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMiRnaMergedMedianZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mirna-merged-median-zscores";
            
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
     * Gene expression mRNA outliers step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMRnaOutliersStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mrna-outliers";
            
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
     * Gene expression capture step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionCaptureStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "capture-gene-expression";
            
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
     * Gene expression capture z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionCaptureZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "capture-gene-expression-zscores";
            
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
     * Gene expression other z-scores step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionOtherZscoresStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "other-gene-expression-zscores";
            
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
     * Gene expression mRNAseq FPKM step decider.
     * Sets flow execution status to SKIP if meta file and data files do not exist
     * 
     * @return JobExecutionDecider
     */
    @Bean
    public JobExecutionDecider geneExpressionMRnaSeqFpkmStepDecider() {
        return (JobExecution jobExecution, StepExecution stepExecution) -> {
            MultiKeyMap datatypeMetadata = (MultiKeyMap) jobExecution.getExecutionContext().get("datatypeMetadata");
            String datatype = "mrna-seq-fpkm";
            
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

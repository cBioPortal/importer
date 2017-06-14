## Outline:
* [Introduction](#introduction)
* [Importer Workflow](#importer-workflow)
* [Job Configuration Specs](#job-configuration-specs)
* [Step Configuration Specs](#step-configuration-specs)

# [Introduction](#introduction)
The Spring Batch importer comprises one job with a series of steps that are executed sequentially or in parallel depending on their datatype. For reference, the importer workflow diagram can be viewed [here](ImporterWorkflowDiagram.pdf).

The `application.properties` file requires the following: 
* `spring.batch.job.enabled`=false
* `chunk.interval`= some integer that will be set as the chunk processing interval value
* `db.user`= the db username
* `db.password`= the db password
* `db.driver`= the driver classname
* `db.connection_string`= the db connection string
* `db.portal_db_name`= the db name
* `db.url`= the db url
* `db.version=${db.version}`: db.version is defined in the master pom file

The user must pass one argument when running the importer (`-s`, `--staging`) which will be added to the `JobExecutionContext` as a `JobParameter` called `stagingDirectory`. The following is an example command for importing a cancer study at a specified directory:

```
$JAVA_HOME/bin/java -jar importer/target/importer-0.1.0.jar -s /path/to/cancer/study
```

# [Importer Workflow](#importer-workflow):
The following describes the components of the batch importer job and its steps. 

## [Job Configuration Specs](#job-configuration-specs):
* `JobExecutionListener`: Used for letting user know whether cancer study data successfully imported or not.
* `JobExecutionDecider`: Checks the DB schema version and stops the job if the DB version does not match what is expected for the portal. If the DB schema does not match what is expected then the `JobExecutionDecider` sets the `FlowExecutionStatus` to **STOPPED**. Otherwise, the `FlowExecutionStatus` is set to **CONTINUE**.
* `Flow`: Defines the conditional flow for importing a cancer study based on the `FlowExecutionStatus` from the `JobExecutionDecider`. On **STOPPED**, the job execution is halted and the application closes. On **CONTINUE**, the job attempts to import a cancer study based on the staging directory passed on the command line. 

## [Step Configuration Specs](#step-configuration-specs):
* [Cancer Study Step](#cancer-study-step)
* [Cancer Study Datatype Metadata](#cancer-study-datatype-metadata)
* [Clinical Data Step](#clinical-data-step)
* [Timeline Data Step](#timeline-data-step)
* [Mutsig Data Step](#mutsig-data-step)
* [Copy Number Segment Data Step](#copy-number-segment-data-step)
* [Gistic Data Step](#gistic-data-step)
* [Profile Datatypes](#profile-datatypes):
  * [Protein-level Data Step](#protein-level-data-step)
  * [CNA Data Step](#cna-data-step)
  * [Gene Expression Data Step](#gene-expression-data-step)
  * [Methylation Data Step](#methylation-data-step)
  * [Mutation Data Step](#mutation-data-step)
  * [Fusion Data Step](#fusion-data-step)
  * [Structural Variant Data Step](#structural-variant-data-step)
* [Case List Step](#case-list-step)

### [Cancer Study Step](#cancer-study-step):
Importing cancer study data will be executed by the `importCancerStudy` Step (executed by the main batch importer job described above.
* `StepExecutionListener`: Used for setting the `ExitStatus` to **STOPPED** after the step has executed if the cancer study metadata could not be imported or if the cancer study metadata could not be loaded from `meta_study.txt`.
* `Tasklets`: A tasklet loads cancer study metadata from `meta_study.txt`. If a cancer study already exists by the cancer study identifier then the existing study is deleted and the new study is imported. Another tasklet searches the cancer study directory for metafiles for all datatypes before executing any datatype import steps. If metafiles and datafiles are found for a datatype, then its import status is set to **true**, otherwise it is set to **false**. The import status will be used by each datatype's `JobExecutionDecider` to determine whether or not the datatype's import step should execute.
* `JobExecutionDecider`: Used to determine whether the import job should continue or not. If the cancer study metadata was successfully loaded and imported into the DB then the `FlowExecutionStatus` is set to **CONTINUE**, otherwise the `FlowExecutionStatus` is set to **STOPPED**.
* `Flow`: Defines the conditional flow for importing other datatypes for a cancer study based on the `FlowExecutionStatus` from the `JobExecutionDecider`. If the `FlowExecutionStatus` is set to **CONTINUE** then the job will continue executing by first loading and importing clinical data and then executing the remaining import steps in parallel. 

### [Cancer Study Datatype Metadata](#cancer-study-datatype-metadata)
Before data is imported for each datatype, a tasklet will be executed by the `loadCancerStudyDatatypeMetadata` Step, which will: 

1. iterate through each datatype and search for a datatype's meta filename
2. load properties from a datatype's metafile
3. search for data filenames matching the `data_filename` specified in the metafile properties

A MultiKeyMap is used to store whether a datatype's metafile and datafile(s) exist in the current cancer study path. If so, then the key `importData` is set to **true**, otherwise it is set to **false**. If **true** then the properties and a list of datafile(s) for the datatype are stored in the MultiKeyMap as well. In the cases for datatypes such as `clinical-supp` where it shares the same meta filename as `clinical`, the tasklet will search the cancer study path for a default data filename pattern (`data_clinical_supp*.txt`). Other cases include `mutation-germline` and `mutation-manual`, which share the same meta filename as `mutation`. After checking the cancer study path for each datatype, the MultiKeyMap is then added to the `JobExecutionContext` to be used by the individual datatype `JobExecutionDecider`s described below.

### [Common Step Components for Datatypes](#common-step-components): 
Each Step for all datatypes will have a `JobExecutionDecider` that will search the cancer study path for a datatype's meta filename and date filename pattern. If the meta file exists and data files can be found then the `FlowExecutionStatus` is set to **RUN** and the current step name is injected into the `JobExecutionContext`, otherwise the `FlowExecutionStatus` is set to **SKIP**.

### [Clinical Data Step](#clinical-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|       Datatype       |         Meta Filename         |           Data Filename            |     Status    |        Step Name       |
| -------------------- | ----------------------------- | ---------------------------------- | ------------- | ---------------------- |
| clinical             | meta_clinical.txt             | data_clinical.txt                  | **Complete**  | clinicalStep           |
| clinical-patient     | meta_clinical_patient.txt     | data_clinical_patient.txt          | **Complete**  | clinicalPatientStep    |
| clinical-sample      | meta_clinical_sample.txt      | data_clinical_sample.txt           | **Complete**  | clinicalSampleStep     |
| bcr-clinical         | meta_bcr_clinical.txt         | data_bcr_clinical.txt              | **Complete**  | bcrClinicalStep        |
| bcr-clinical-patient | meta_bcr_clinical_patient.txt | data_bcr_clinical_data_patient.txt | **Complete**  | bcrClinicalPatientStep |
| bcr-clinical-sample  | meta_bcr_clinical_sample.txt  | data_bcr_clinical_data_sample.txt  | **Complete**  | bcrClinicalSampleStep  |
| clinical-supp        | meta_clinical.txt             | data_clinical_supp*.txt            | **Complete**  | clinicalSuppStep       |
| clinical-caises      | meta_clinical_caises.txt      | data_clinical_caises.xml           | _In Progress_ | --                     |

A main Flow (`clinicalStepFlow`) will execute the Step(s) for importing each clinical datatype sequentially. All clinical datatype steps will have the following components in common: 
* `Tasklet`: A universal tasklet loads clinical attributes from a list of clinical datafiles based on the data filename corresponding to the `currentStep` stored in the `JobExecutionContext`. It is assumed that metadata for clinical attributes are stored at the top of the file and that the clinical datafile(s) have passed validation. A HashMap of data filenames and clinical attributes loaded are injected into the `JobExecutionContext` to be used by the clinical data readers, processors, and writers. 
* `Step`: A universal step builder for clinical data implements the reading, processing, and writing of clinical data using the HashMap of datafile clinical attributes loaded from the `Tasklet` above. 
* `StepExecutionListener`: Before the clinical datatype step executes, the listener will add the cancer study imported from [Cancer Study Step](#cancer-study-step) to the `StepExecutionContext`, as well as the datafile clinical attributes loaded from the `Tasklet` described above. After the clinical datatype step executes, the listener will report the total records imported into PATIENT, SAMPLE, CLINICAL_PATIENT, and CLINICAL_SAMPLE, how many new clinical attributes were imported into CLINICAL_ATTRIBUTE, and how many rollbacks or skips occurred during import.

### [Timeline Data Step](#timeline-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|    Datatype    |   Meta Filename   |   Data Filename    |    Status    |  Step Name   |
| -------------- | ----------------- | ------------------ | ------------ | ------------ |
| time-line-data | meta_timeline.txt | data_timeline*.txt | **Complete** | timelineStep |

A main Flow (`timelineStepFlow`) will execute the Step(s) for importing timeline data. Components for the timeline step flow include:
* `Tasklet`: A tasklet loads timeline metadata (header) from the datafile and checks if header is valid.
* `JobExecutionDecider`: In addition to the `JobExecutionDecider` described above, an additional `JobExecutionDecider` is used to determine whether the timeline metadata was loaded properly. If so then the `FlowExecutionStatus` is set to **CONTINUE**, otherwise the `FlowExecutionStatus` is set to **STOPPED**.
* `Step`: A Step to load timeline metadata is executed if both the timeline metafile and datafiles exist. A second Step implements the importing of timeline data from the datafiles.
* `StepExecutionListener`: Before the timeline data import step executes, the listener will add the cancer study imported from [Cancer Study Step](#cancer-study-step) to the `StepExecutionContext`, datafile list, and timeline metadata to the `StepExecutionContext`. After the timeline data import step executes, the listener will report how many genes were loaded from the datafile, the number of entries skipped, and total records imported into CLINICAL_EVENT and CLINICAL_EVENT_DATA.

### [Mutsig Data Step](#mutsig-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|         Datatype         |   Meta Filename |  Data Filename  |    Status    | Step Name  |
| ------------------------ | --------------- | --------------- | ------------ | ---------- |
| mutation-significance-v2 | meta_mutsig.txt | data_mutsig.txt | **Complete** | mutSigStep |

A main Flow (`mutSigStepFlow`) will execute the Step(s) for importing mutsig data. Components for the mutsig step flow include:
* `Tasklet`: A tasklet loads mutsig metadata (header, number of records) from the datafile and adds the metadata to the `JobExecutionContext`.
* `JobExecutionDecider`: In addition to the `JobExecutionDecider` described above, an additional `JobExecutionDecider` is used to determine whether the mutsig metadata was loaded properly. If so then the `FlowExecutionStatus` is set to **CONTINUE**, otherwise the `FlowExecutionStatus` is set to **STOPPED**.
* `Step`: A Step to load mutsig metadata is executed if both the mutsig metafile and datafile exist. A second Step implements the reading, processing, and importing of mutsig data from the datafile.
* `StepExecutionListener`: Before the mutsig data import step executes, the listener will add the cancer study imported to the `StepExecutionContext`, datafile, and mutsig metadata to the `StepExecutionContext`. After the mutsig data import step executes, the listener will report how many genes were loaded from the datafile, the number of entries skipped, and total records imported into MUT_SIG.

### [Copy Number Segment Data Step](#copy-number-segment-data-step):
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|   Datatype   |                   Meta Filename                   |                     Data Filename                 |    Status    |           Step Name           |
| ------------ | ------------------------------------------------- | ------------------------------------------------- | ------------ | ----------------------------- |
| cna-hg19-seg | `<CANCER_STUDY_IDENTIFIER>`_meta_cna_hg19_seg.txt | `<CANCER_STUDY_IDENTIFIER>`_data_cna_hg19_seg.txt | **Complete** | copyNumberSegmentHg19DataStep |
| cna-hg18-seg | `<CANCER_STUDY_IDENTIFIER>`_meta_cna_hg18_seg.txt | `<CANCER_STUDY_IDENTIFIER>`_data_cna_hg18_seg.txt | **Complete** | copyNumberSegmentHg18DataStep |

A main Flow (`copyNumberSegStepFlow`) will execute the Step(s) for importing copy number segment data. Components for the copy number segment flow include: 
* `Tasklet`: A tasklet imports a copy number segment file record loaded from the current datatype's metafile into COPY_NUMBER_SEG_FILE and also loads copy number segment metadata (header, number of records) from the datafile and adds the metadata to the `JobExecutionContext`. These information are used by the copy number segment data reader and writers.
* `Step`: A universal step builder for copy number segment data implements the reading, processing, and writing of copy number segment data using the MultiKeyMap of the datafile metadata (header, number of records) loaded from the `Tasklet` above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the copy number segment datatype step executes, the listener will add the data loaded from the `Tasklet` above to the `StepExecutionContext`. After the copy number segment datatype step executes, the listener will report the total samples and entries skipped, as well as the total records imported into COPY_NUMBER_SEG for the current datatype.

### [Gistic Data Step](#gistic-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|     Datatype     |       Meta Filename       |       Data Filename       |    Status    |     Step Name      |
| ---------------- | ------------------------- | ------------------------- | ------------ | ------------------ |
| gistic-genes-amp | meta_gistic_genes_amp.txt | data_gistic_genes_amp.txt | **Complete** | gisticGenesAmpStep |
| gistic-genes-del | meta_gistic_genes_del.txt | data_gistic_genes_del.txt | **Complete** | gisticGenesDelStep |

A main Flow (`gisticGenesStepFlow`) will execute the Step(s) for importing gistic genes data. Components for the gistic genes flow include: 
* `Tasklet`: A loads gistic genes metadata (header, number of records) from the datafile and adds the metadata to the `JobExecutionContext`. These information are used by the gistic genes data reader and writers.
* `Step`: A universal step builder for gistic genes data implements the reading and writing of gistic genes data using the MultiKeyMap of the datafile metadata (header, number of records) loaded from the `Tasklet` above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the gistic genes datatype step executes, the listener will add the data loaded from the `Tasklet` above to the `StepExecutionContext`. After the gistic genes datatype step executes, the listener will report the total entries skipped, total genes loaded, and the total records imported into GISTIC and GISTIC_TO_GENE for the current datatype.


## [Profile Datatypes](#profile-datatypes):

### [Protein-level Data Step](#protein-level-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|        Datatype        |           Meta Filename         |           Data Filename         |    Status    |         Step Name         |
| ---------------------- | ------------------------------- | ------------------------------- | ------------ | ------------------------- |
| rppa                   | meta_rppa.txt                   | data_rppa.txt                   | **Complete** | rppaStep                  |
| rppa-zscores           | meta_rppa_Zscores.txt           | data_rppa_Zscores.txt           | **Complete** | rppaZscoresStep           |
| protein-quantification | meta_protein_quantification.txt | data_protein_quantification.txt | **Complete** | proteinQuantificationStep |
 
A main Flow (`proteinLevelStepFlow`) will execute the Step(s) for importing each protein-level datatype sequentially. All protein-level datatype steps will have the following components in common: 
* `Tasklets`: Two universal tasklets are used for protein-level data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the profile metadata (header, non-case id columns, case id maps, normal case ids). These information are used by the profile data reader, as well as the protein-level data processors and writers. 
* `Step`: A universal step builder for protein-level data implements the reading and writing of protein-level data using the genetic profile and the MultiKeyMap of datafile profile metadata (header, non-case id columns, case id maps, normal case ids) loaded from the `Tasklet`s above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the protein-level datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the protein-level datatype step executes, the listener will report the total genes loaded, samples skipped, and entries skipped, as well as the total records imported into GENETIC_ALTERATION for the current datatype.

### [CNA Data Step](#cna-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|      Datatype     |       Meta Filename     |       Data Filename     |    Status    |     Step Name     |
| ----------------- | ----------------------- | ----------------------- | ------------ | ----------------- |
| cna-gistic        | meta_CNA.txt            | data_CNA.txt            | **Complete** | cnaStep           |
| cna-foundation    | meta_CNA_foundation.txt | data_CNA_foundation.txt | **Complete** | cnaFoundationStep |
| cna-rae           | meta_CNA_RAE.txt        | data_CNA_RAE.txt        | **Complete** | cnaRaeStep        |
| cna-consensus     | meta_CNA_consensus.txt  | data_CNA_consensus.txt  | **Complete** | cnaConsensusStep  |
| linear-cna-gistic | meta_linear_CNA.txt     | data_linear_CNA.txt     | **Complete** | cnaLinearStep     |
| log2-cna          | meta_log2CNA.txt        | data_log2CNA.txt        | **Complete** | cnaLog2Step       |

A main Flow (`cnaStepFlow`) will execute the Step(s) for importing each CNA data type sequentially. All CNA datatype steps will have the following components in common: 
* `Tasklets`: Two universal tasklets are used for CNA data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the profile metadata (header, non-case id columns, case id maps, normal case ids). These information are used by the profile data reader, and writers, as well as the CNA data processors. 
* `Step`: A universal step builder for CNA data implements the reading, processing, and writing of CNA data using the genetic profile and the MultiKeyMap of datafile profile metadata (header, non-case id columns, case id maps, normal case ids) loaded from the `Tasklet`s above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the CNA datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the CNA datatype step executes, the listener will report the total genes loaded, samples skipped, and entries skipped, as well as the total records imported into GENETIC_ALTERATION, CNA_EVENT, SAMPLE_CNA_EVENT for the current datatype.

### [Gene Expression Data Step](#gene-expression-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|              Datatype              |                 Meta Filename               |                 Data Filename               |     Status    |                 Step Name                  |
| ---------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------- | ------------------------------------------ |
| affymetrix-gene-expression         | meta_expression.txt                         | data_expression.txt                         | **Complete**  | geneExpressionAffymetrixStep               |
| affymetrix-gene-expression-zscores | meta_expression_Zscores.txt                 | data_expression_Zscores.txt                 | **Complete**  | geneExpressionAffymetrixZscoresStep        |
| gene-expression-merged             | meta_expression_merged.txt                  | data_expression_merged.txt                  | **Complete**  | geneExpressionMergedStep                   |
| gene-expression-merged-zscores     | meta_expression_merged_Zscores.txt          | data_expression_merged_Zscores.txt          | **Complete**  | geneExpressionMergedZscoresStep            |
| rnaseq-gene-expression             | meta_RNA_Seq_expression_median.txt          | data_RNA_Seq_expression_median.txt          | **Complete**  | geneExpressionRnaSeqStep                   |
| rnaseq-gene-expression-zscores     | meta_RNA_Seq_mRNA_median_Zscores.txt        | data_RNA_Seq_mRNA_median_Zscores.txt        | **Complete**  | geneExpressionRnaSeqZscoresStep            |
| agilent-gene-expression            | meta_expression_median.txt                  | data_expression_median.txt                  | **Complete**  | geneExpressionAgilentStep                  |
| agilent-gene-expression-zscores    | meta_mRNA_median_Zscores.txt                | data_mRNA_median_Zscores.txt                | **Complete**  | geneExpressionAgilentZscoresStep           |
| rnaseq-v2-gene-expression          | meta_RNA_Seq_v2_expression_median.txt       | data_RNA_Seq_v2_expression_median.txt       | **Complete**  | geneExpressionRnaSeqV2Step                 |
| rnaseq-v2-gene-expression-zscores  | meta_RNA_Seq_v2_mRNA_median_Zscores.txt     | data_RNA_Seq_v2_mRNA_median_Zscores.txt     | **Complete**  | geneExpressionRnaSeqV2ZscoresStep          |
| mirna-expression                   | meta_expression_miRNA.txt                   | data_expression_miRNA.txt                   | **Complete**  | geneExpressionMiRnaStep                    |
| mirna-median-zscores               | meta_miRNA_median_Zscores.txt               | data_miRNA_median_Zscores.txt               | **Complete**  | geneExpressionMiRnaMedianZscoresStep       |
| mirna-merged-median-zscores        | meta_expression_merged_median_Zscores.txt   | data_expression_merged_median_Zscores.txt   | **Complete**  | geneExpressionMiRnaMergedMedianZscoresStep |
| mrna-outliers                      | meta_mRNA_outliers.txt                      | data_mRNA_outliers.txt                      | **Complete**  | geneExpressionMRnaOutliersStep             |
| capture-gene-expression            | meta_RNA_Seq_expression_capture.txt         | data_RNA_Seq_expression_capture.txt         | **Complete**  | geneExpressionCaptureStep                  |
| capture-gene-expression-zscores    | meta_RNA_Seq_expression_capture_Zscores.txt | data_RNA_Seq_expression_capture_Zscores.txt | **Complete**  | geneExpressionCaptureZscoresStep           |
| other-gene-expression-zscores      | meta_expression_other_Zscores.txt           | data_expression_other_Zscores.txt           | **Complete**  | geneExpressionOtherZscoresStep             |
| mrna-seq-fpkm                      | meta_mRNA_seq_fpkm.txt                      | data_mRNA_seq_fpkm.txt                      | **Complete**  | geneExpressionMRnaSeqFpkmStep              |
| mrna-seq-rsem                      | meta_mrnaseq_rsem.txt                       | data_mrnaseq_rsem.txt                       | _In Progress_ | --                                         |
| mrna-seq-fcount                    | meta_mrnaseq_fcount.txt                     | data_mrnaseq_fcount.txt                     | _In Progress_ | --                                         |
_* **Note:** Support for_ `mrna-seq-rsem` _and_ `mrna-seq-fcount` _will be added after meta datatypes are resolved._

A main Flow (`geneExpressionFlow`) will execute the Step(s) for importing each gene expression datatype sequentially. All gene expression datatype steps will have the following components in common:
* `Tasklets`: Two universal tasklets are used for protein-level data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the profile metadata (header, non-case id columns, case id maps, normal case ids). These information are used by the profile data reader, as well as the protein-level data processors and writers. 
* `Step`: A universal step builder for gene expression data implements the reading and writing of gene expression data using the genetic profile and the MultiKeyMap of datafile profile metadata (header, non-case id columns, case id maps, normal case ids) loaded from the `Tasklet`s above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the gene expression datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the gene expression datatype step executes, the listener will report the total genes loaded, samples skipped, and entries skipped, as well as the total records imported into GENETIC_ALTERATION for the current datatype.

As of now, dependencies are being ignored for any gene expression datatypes that require them as the importer is assuming that any conversion have already taken place and that z-scores have been generated.

### [Methylation Data Step](#methylation-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|      Datatype      |         Meta Filename       |         Data Filename       |     Status    |       Step Name       |
| ------------------ | --------------------------- | --------------------------- | ------------- | --------------------- |
| methylation-hm27   | meta_methylation_hm27.txt   | data_methylation_hm27.txt   | **Complete**  | methylationHm27Step   |
| methylation-hm450  | meta_methylation_hm450.txt  | data_methylation_hm450.txt  | **Complete**  | methylationHm450Step  |
| methylation-binary | meta_methylation_binary.txt | data_methylation_binary.txt | _In Progress_ | --                    |
_* **Note:** Support for_ `methylation-binary` _will be added after meta datatype is resolved._

A main Flow (`methylationStepFlow`) will execute the Step(s) for importing each methylation data type sequentially. All methylation datatype steps will have the following components in common: 
* `Tasklets`: Two universal tasklets are used for methylation data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the profile metadata (header, non-case id columns, case id maps, normal case ids). These information are used by the profile data reader and writer. 
* `Step`: A universal step builder for methylation data implements the reading, processing, and writing of methylation data using the genetic profile and the MultiKeyMap of datafile profile metadata (header, non-case id columns, case id maps, normal case ids) loaded from the `Tasklet`s above. A report of records loaded is printed after each file is read.
* `StepExecutionListener`: Before the methylation datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the methylation datatype step executes, the listener will report the total genes loaded, samples skipped, and entries skipped, as well as the total records imported into GENETIC_ALTERATION for the current datatype.

### [Mutation Data Step](#mutation-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|       Datatype      |         Meta Filename       |              Data Filename             |     Status   |        Step Name       |
| ------------------- | --------------------------- | -------------------------------------- | ------------ | ---------------------- |
| mutation            | meta_mutations_extended.txt | data_mutations_extended*.txt           | **Complete** | mutationStep           |
| mutation-germline   | meta_mutations_extended.txt | data_mutations_germline.txt            | **Complete** | mutationGermlineStep   |
| mutation-manual     | meta_mutations_extended.txt | data_mutations_manual.txt              | **Complete** | mutationManualStep     |

A main Flow (`mutationStepFlow`) will execute the Step(s) for importing each mutation datatype sequentially. All mutation datatype steps will have the following components in common:
* `Tasklets`: Two universal tasklets are used for mutation data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the headers of each MAF file and number of records in the file (row count). These information are used by the mutation data readers, processors, and writers. 
* `Step`: A universal step builder for mutation data implements the reading, processing, and writing of mutation data using the genetic profile and the MultiKeyMap of MAF file metadata (headers and number of records in file) loaded from the `Tasklet`s above. A report of mutations filtered out is printed after each file is read.
* `StepExecutionListener`: Before the mutation datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the mutation datatype step executes, the listener will report the total samples loaded, total genes loaded, samples skipped, and entries skipped, as well as the total records imported into MUTATION and MUTATION_EVENT for the current datatype. 


### [Fusion Data Step](#fusion-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

| Datatype |   Meta Filename  |   Data Filename  |    Status    | Step Name  |
| -------- | ---------------- | ---------------- | ------------ | ---------- |
| fusion   | meta_fusions.txt | data_fusions.txt | **Complete** | fusionStep |

A main Flow (`fusionStepFlow`) will execute the Step(s) for importing each fusion datatype sequentially. All fusion datatype steps will have the following components in common:
* `Tasklets`: Two universal tasklets are used for fusion data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the headers of each fusion file and number of records in the file (row count). These information are used by the fusion data readers, processors, and writers. 
* `Step`: A step fusion data implements the reading, processing, and writing of fusion data using the genetic profile and the MultiKeyMap of fusion file metadata (headers and number of records in file) loaded from the `Tasklet`s above.
* `StepExecutionListener`: Before the fusion datatype step executes, the listener will add the data loaded from the `Tasklet`s above to the `StepExecutionContext`. After the fusion datatype step executes, the listener will report the total samples loaded, total genes loaded, samples skipped, and entries skipped, as well as the total records imported into MUTATION and MUTATION_EVENT for the current datatype. 

### [Structural Variant Data Step](#structural-variant-data-step)
The following datatype(s) are loaded and imported sequentially if metafiles are found for them: 

|      Datatype      | Meta Filename | Data Filename |    Status    |       Step Name       |
| ------------------ | ------------- | ------------- | ------------ | --------------------- |
| structural-variant | meta_SV.txt   | data_SV.txt   | **Complete** | structuralVariantStep |

A main Flow (`structuralVariantStepFlow`) will execute the Step(s) for importing structural variant data. All structural variant datatype steps will have the following components in common: 
* `Tasklet`: Two universal tasklets are used for structural variant data steps. One tasklet loads and imports a genetic profile from a datatype's metafile and injects the genetic profile into the `JobExecutionContext`. The second tasklet injects the list of datafiles into the `JobExecutionContext`, as well as a MultiKeyMap of the headers of each datafile and number of records in the file (row count). These information are used by the structural variant data reader, processor, and writer. 
* `Step`: A step for structural variant data implements the reading and writing of structural variant data using the genetic profile and the MultiKeyMap of structural variant file metadata (headers and number of records in file) loaded from the `Tasklet` above.
* `StepExecutionListener`: Before the structural variant datatype step executes, the listener will add the data loaded from the `Tasklet` above to the `StepExecutionContext`. After the structural variant datatype step executes, the listener will report the total number of samples and entries skipped, as well as the total records imported into STRUCTURAL_VARIANT for the current datatype. 

### [Case List Step](#case-list-step)
Case lists are imported after every datatype importing step is executed. The following components make up the case list Step:
* `Reader`: A reader looks for the `case_lists` directory and will search for default (standard) case list filenames. If the case list file does not have the `case_list_ids` property set then the list of case ids that were imported for the case list staging files will be used instead (i.e., for the case list file `cases_sequenced.txt`, if the `case_list_ids` property is not defined then the set of case ids loaded during the `mutations` step will be used instead).
* `Writer`: A writer imports the case lists found by the reader.
* `StepExecutionListener`: The listener will add datatype metadata and the cancer study imported to the `StepExecutionContext` before the Step executes. After the Step executes, the listener reports how many records were imported into SAMPLE_LIST and SAMPLE_LIST_LIST.



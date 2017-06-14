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

package org.cbio.portal.pipelines.importer.config;

import org.apache.commons.collections.map.*;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
public class DatatypeMetadata {
    
    /**
     * All datatype metadata configurations.
     * 
     * @return MultiKeyMap
     */
    @Bean(name="datatypeMetadataMap")
    public MultiKeyMap datatypeMetadataMap() {
        MultiKeyMap datatypeMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        datatypeMetadataMap.putAll(clinicalMetadata());
        datatypeMetadataMap.putAll(timelineMetadata());
        datatypeMetadataMap.putAll(mutSigMetadata());
        datatypeMetadataMap.putAll(copyNumberSegmentMetadata());
        datatypeMetadataMap.putAll(gisticMetadata());
        datatypeMetadataMap.putAll(proteinLevelMetadata());
        datatypeMetadataMap.putAll(cnaMetadata());
        datatypeMetadataMap.putAll(geneExpressionMetadata());
        datatypeMetadataMap.putAll(methylationMetadata());
        datatypeMetadataMap.putAll(mutationMetadata());
        datatypeMetadataMap.putAll(fusionMetadata());
        datatypeMetadataMap.putAll(structuralVariantMetadata());
        
        return datatypeMetadataMap;
    }    
    
    /**
     * Clinical datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap clinicalMetadata() {
        MultiKeyMap clinicalMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // clinical datatype metadata
        clinicalMetadataMap.put("clinical", "meta_filename", "meta_clinical.txt");
        
        // clinical-patient datatype metadata
        clinicalMetadataMap.put("clinical-patient", "meta_filename", "meta_clinical_patient.txt");
        
        // clinical-sample  datatype metadata
        clinicalMetadataMap.put("clinical-sample", "meta_filename", "meta_clinical_sample.txt");
        
        // bcr-clinical datatype metadata
        clinicalMetadataMap.put("bcr-clinical", "meta_filename", "meta_bcr_clinical.txt");
        
        // bcr-clinical-patient datatype metadata
        clinicalMetadataMap.put("bcr-clinical-patient", "meta_filename", "meta_bcr_clinical_patient.txt");
        
        // bcr-clinical-sample  datatype metadata
        clinicalMetadataMap.put("bcr-clinical-sample", "meta_filename", "meta_bcr_clinical_sample.txt");
        
        // clinical-supp datatype metadata
        clinicalMetadataMap.put("clinical-supp", "meta_filename", "meta_clinical_supp.txt");
        
        return clinicalMetadataMap;
    }
    
    /**
     * Timeline datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap timelineMetadata() {
        MultiKeyMap timelineMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // time-line datatype metadata
        timelineMetadataMap.put("time-line-data", "meta_filename", "meta_timeline.txt");
        
        return timelineMetadataMap;
    }
    
    /**
     * Genetic profile datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap mutSigMetadata() {
        MultiKeyMap mutSigMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // mutsig datatype metadata
        mutSigMetadataMap.put("mutation-significance-v2", "meta_filename", "meta_mutsig.txt");
        
        return mutSigMetadataMap;
    }
    
    /**
     * Copy number segment datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap copyNumberSegmentMetadata() {
        MultiKeyMap copyNumberSegmentMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // copy number segment datatype metadata for reference genome hg19
        copyNumberSegmentMetadataMap.put("cna-hg19-seg", "meta_filename", "<CANCER_STUDY>_meta_cna_hg19_seg.txt");
        
        // copy number segment datatype metadata for reference genome hg18
        copyNumberSegmentMetadataMap.put("cna-hg18-seg", "meta_filename", "<CANCER_STUDY>_meta_cna_hg18_seg.txt");

        return copyNumberSegmentMetadataMap;
    }
    
    /**
     * Gistic datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap gisticMetadata() {
        MultiKeyMap gisticMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // gistic datatype metadata
        gisticMetadataMap.put("gistic-genes-amp", "meta_filename", "meta_gistic_genes_amp.txt");
        
        // gistic datatype metadata
        gisticMetadataMap.put("gistic-genes-del", "meta_filename", "meta_gistic_genes_del.txt");

        return gisticMetadataMap;
    }
    
    /**
     * Protein-level datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap proteinLevelMetadata() {
        MultiKeyMap proteinLevelMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // rppa datatype metadata
        proteinLevelMetadataMap.put("rppa", "meta_filename", "meta_rppa.txt");
        
        // rppa z-scores datatype metadata
        proteinLevelMetadataMap.put("rppa-zscores", "meta_filename", "meta_rppa_Zscores.txt");
        
        // protein-quantification datatype metadata
        proteinLevelMetadataMap.put("protein-quantification", "meta_filename", "meta_protein_quantification.txt");
        
        return proteinLevelMetadataMap;
    }
    
    /**
     * CNA datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap cnaMetadata() {
        MultiKeyMap cnaMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // cna-gistic datatype metadata
        cnaMetadataMap.put("cna-gistic", "meta_filename", "meta_CNA.txt");
        
        // cna-foundation datatype metadata
        cnaMetadataMap.put("cna-foundation", "meta_filename", "meta_CNA_foundation.txt");
        
        // cna-rae datatype metadata
        cnaMetadataMap.put("cna-rae", "meta_filename", "meta_CNA_RAE.txt");
        
        // cna-consensus datatype metadata
        cnaMetadataMap.put("cna-consensus", "meta_filename", "meta_CNA_consensus.txt");
        
        // linear-cna-gistic metadata
        cnaMetadataMap.put("linear-cna-gistic", "meta_filename", "meta_linear_CNA.txt");
        
        // log2-cna metadata
        cnaMetadataMap.put("log2-cna", "meta_filename", "meta_log2CNA.txt");
        
        return cnaMetadataMap;
    }
    
    /**
     * Gene expression datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap geneExpressionMetadata() {
        MultiKeyMap geneExpressionMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // affymetrix-gene-expression datatype metadata
        geneExpressionMetadataMap.put("affymetrix-gene-expression", "meta_filename", "meta_expression.txt");
                
        // affymetrix-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("affymetrix-gene-expression-zscores", "meta_filename", "meta_expression_Zscores.txt");
        
        // gene-expression-merged datatype metadata
        geneExpressionMetadataMap.put("gene-expression-merged", "meta_filename", "meta_expression_merged.txt");

        // gene-expression-merged-zscores datatype metadata
        geneExpressionMetadataMap.put("gene-expression-merged-zscores", "meta_filename", "meta_expression_merged_Zscores.txt");

        // rnaseq-gene-expression datatype metadata
        geneExpressionMetadataMap.put("rnaseq-gene-expression", "meta_filename", "meta_RNA_Seq_expression_median.txt");
        
        // rnaseq-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("rnaseq-gene-expression-zscores", "meta_filename", "meta_RNA_Seq_mRNA_median_Zscores.txt");

        // agilent-gene-expression datatype metadata
        geneExpressionMetadataMap.put("agilent-gene-expression", "meta_filename", "meta_expression_median.txt");
        
        // agilent-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("agilent-gene-expression-zscores", "meta_filename", "meta_mRNA_median_Zscores.txt");

        // rnaseq-v2-gene-expression datatype metadata
        geneExpressionMetadataMap.put("rnaseq-v2-gene-expression", "meta_filename", "meta_RNA_Seq_v2_expression_median.txt");

        // rnaseq-v2-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("rnaseq-v2-gene-expression-zscores", "meta_filename", "meta_RNA_Seq_v2_mRNA_median_Zscores.txt");

        // mirna-expression datatype metadata
        geneExpressionMetadataMap.put("mirna-expression", "meta_filename", "meta_expression_miRNA.txt");

        // mirna-median-zscores datatype metadata
        geneExpressionMetadataMap.put("mirna-median-zscores", "meta_filename", "meta_miRNA_median_Zscores.txt");

        // mirna-merged-median-zscores datatype metadata
        geneExpressionMetadataMap.put("mirna-merged-median-zscores", "meta_filename", "meta_expression_merged_median_Zscores.txt");

        // mrna-outliers datatype metadata
        geneExpressionMetadataMap.put("mrna-outliers", "meta_filename", "meta_mRNA_outliers.txt");

        // capture-gene-expression datatype metadata
        geneExpressionMetadataMap.put("capture-gene-expression", "meta_filename", "meta_RNA_Seq_expression_capture.txt");

        // capture-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("capture-gene-expression-zscores", "meta_filename", "meta_RNA_Seq_expression_capture_Zscores.txt");

        // other-gene-expression-zscores datatype metadata
        geneExpressionMetadataMap.put("other-gene-expression-zscores", "meta_filename", "meta_expression_other_Zscores.txt");
        
        // mrna-seq-fpkm datatype metadata
        geneExpressionMetadataMap.put("mrna-seq-fpkm", "meta_filename", "meta_mRNA_seq_fpkm.txt");
        
        return geneExpressionMetadataMap;
    }    
    
    /**
     * Methylation datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap methylationMetadata() {
        MultiKeyMap methylationMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // methylation-hm27 datatype metadata
        methylationMetadataMap.put("methylation-hm27", "meta_filename", "meta_methylation_hm27.txt");
        
        // methylation-hm450 datatype metadata
        methylationMetadataMap.put("methylation-hm450", "meta_filename", "meta_methylation_hm450.txt");
        
        return methylationMetadataMap;
    }
    
    /**
     * Mutation datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap mutationMetadata() {
        MultiKeyMap mutationMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // mutation datatype metadata
        mutationMetadataMap.put("mutation", "meta_filename", "meta_mutations_extended.txt");
        
        // mutation-germline datatype metadata
        mutationMetadataMap.put("mutation-germline", "meta_filename", "meta_mutations_germline.txt");
        
        // mutation-manual datatype metadata
        mutationMetadataMap.put("mutation-manual", "meta_filename", "meta_mutations_manual.txt");
        
        return mutationMetadataMap;
    }
    
    /**
     * Fusion datatype metadata configuration.
     * 
     * @return MultiKeyMap
     */
    public MultiKeyMap fusionMetadata() {
        MultiKeyMap fusionMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // fusion datatype metadata
        fusionMetadataMap.put("fusion", "meta_filename", "meta_fusions.txt");

        return fusionMetadataMap;
    }
    
    public MultiKeyMap structuralVariantMetadata() {
        MultiKeyMap structuralVariantMetadataMap = MultiKeyMap.decorate(new LinkedMap());
        
        // structural variant datatype metadata
        structuralVariantMetadataMap.put("structural-variant", "meta_filename", "meta_SV.txt");
        
        return structuralVariantMetadataMap;
    }
    
}

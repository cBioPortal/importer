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

package org.cbio.portal.pipelines.importer.config.processor;

import org.mskcc.cbio.model.*;
import org.cbio.portal.pipelines.importer.model.*;
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.config.composite.CompositeMutationData;

import org.apache.commons.logging.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class FusionDataProcessor implements ItemProcessor<FusionRecord, CompositeMutationData> {
    
    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(FusionDataProcessor.class);
    
    @Override
    public CompositeMutationData process(FusionRecord fusionRecord) throws Exception {
        // screen fusion record and return null if does not pass screening
        FusionRecord screenedRecord = screenFusionRecord(fusionRecord);
        if (screenedRecord == null) {
            return null;
        }
        
        // extract mutation and mutation event data from screened fusion record
        Mutation mutation = transformFusionRecordToMutation(fusionRecord);
        MutationEvent mutationEvent = transformFusionRecordToMutationEvent(fusionRecord);
        mutation.setMutationEvent(mutationEvent);
        
        // create instance of composite mutation data
        CompositeMutationData cmd = new CompositeMutationData();
        cmd.setMutation(mutation);
        
        return cmd;
    }
    
    /**
     * Transform a fusion record to a mutation instance.
     *
     * @param fusionRecord
     * @return Mutation
     */
    private Mutation transformFusionRecordToMutation(FusionRecord fusionRecord) {
        Mutation mutation = new Mutation();
        mutation.setSampleId(fusionRecord.getSampleId());
        mutation.setGeneticProfileId(fusionRecord.getGeneticProfileId());
        mutation.setEntrezGeneId(Integer.valueOf(fusionRecord.getEntrezGeneId()));
        mutation.setCenter(fusionRecord.getCenter());
        mutation.setSequenceSource("NA");
        
        return mutation;
    }
    
    /**
     * Transform a fusion record to a mutation event instance.
     *
     * @param fusionRecord
     * @return MutationEvent
     */
    private MutationEvent transformFusionRecordToMutationEvent(FusionRecord fusionRecord) {
        MutationEvent mutationEvent = new MutationEvent();
        mutationEvent.setEntrezGeneId(Integer.valueOf(fusionRecord.getEntrezGeneId()));
        mutationEvent.setProteinChange(fusionRecord.getFusion());
        mutationEvent.setMutationType("Fusion");
        
        return mutationEvent;
    }
    
    /**
     * Performs basic data screening to determine whether fusion record is acceptable or not.
     * Returns null if fusion record does not pass screening
     * 
     * @param fusionRecord
     * @return FusionRecord
     */
    private FusionRecord screenFusionRecord(FusionRecord fusionRecord) {
        // check gene symbol and entrez gene id values
        String hugoGeneSymbol = fusionRecord.getHugoSymbol();
        Integer entrezGeneId = !DataFileUtils.isNullOrEmptyValue(fusionRecord.getEntrezGeneId())?
                Integer.valueOf(fusionRecord.getEntrezGeneId()):null;
        if ((DataFileUtils.isNullOrEmptyValue(hugoGeneSymbol) || hugoGeneSymbol.equalsIgnoreCase("unknown")) 
                && (entrezGeneId == null || entrezGeneId <= 0)) {
            LOG.warn("Skipping entry with invalid (Entrez_Gene_Id,Hugo_Symbol): " + 
                        "(" + entrezGeneId + "," + hugoGeneSymbol + ")");
            return null;
        }
        
        // check if gene can be resolved from fusion record first
        Gene gene = geneDataUtils.resolveGeneFromRecordData(entrezGeneId, hugoGeneSymbol, null);
        if (gene == null) {
            LOG.warn("Could not resolve gene from (Entrez_Gene_Id,Hugo_Symbol): " + 
                    "(" + entrezGeneId + "," + hugoGeneSymbol + ")");
            return null;
        }
        // update entrez gene id, hugo gene symbol, and chromosome (if necessary) with resolved gene data
        entrezGeneId = gene.getEntrezGeneId();
        hugoGeneSymbol = gene.getHugoGeneSymbol();
        
        // update fusion record with any resolved values from data checks above
        fusionRecord.setEntrezGeneId(String.valueOf(entrezGeneId));
        fusionRecord.setHugoSymbol(hugoGeneSymbol);
        
        return fusionRecord;
    }
    
}

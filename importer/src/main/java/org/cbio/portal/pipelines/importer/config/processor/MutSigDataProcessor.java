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
import org.cbio.portal.pipelines.importer.model.MutSigRecord;
import org.cbio.portal.pipelines.importer.util.GeneDataUtils;

import org.apache.commons.logging.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutSigDataProcessor implements ItemProcessor<MutSigRecord, MutSig> {

    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(MutSigDataProcessor.class);
    
    @Override
    public MutSig process(MutSigRecord mutSigRecord) throws Exception {
        MutSig screenedRecord = screenMutSigRecord(mutSigRecord);
        
        return screenedRecord;
    }
    
    /**
     * Performs basic data screening to determine whether mutsig record is acceptable or not.
     * Returns null if mutsig record does not pass screening
     * 
     * @param mutSigRecord
     * @return MutSig
     */
    private MutSig screenMutSigRecord(MutSigRecord mutSigRecord) {
        // light-processing for p-value and q-value
        Float pValue = Float.valueOf(mutSigRecord.getpValue().replace("<", ""));
        Float qValue = Float.valueOf(mutSigRecord.getqValue().replace("<", ""));

        // check that hugo gene symbol value
        String hugoGeneSymbol = mutSigRecord.getHugoSymbol();
        Gene gene = geneDataUtils.resolveGeneFromRecordData(hugoGeneSymbol, null);
        if (gene == null) {
            LOG.warn("Could not resolve gene from Hugo_Symbol: " + hugoGeneSymbol);
            return null;
        }
        Integer entrezGeneId = gene.getEntrezGeneId();
        
        // create new MutSig instance with processed/resolved data from above
        MutSig mutSig = new MutSig();
        mutSig.setCancerStudyId(mutSigRecord.getCancerStudyId());
        mutSig.setEntrezGeneId(entrezGeneId);
        mutSig.setRank(Integer.valueOf(mutSigRecord.getRank()));
        mutSig.setNumBasesCovered(Integer.valueOf(mutSigRecord.getNumBasesCovered()));
        mutSig.setNumMutations(Integer.valueOf(mutSigRecord.getNumMutations()));
        mutSig.setPValue(pValue);
        mutSig.setQValue(qValue);
        
        return mutSig;
    }
    
}

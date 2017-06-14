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

package org.cbio.portal.pipelines.importer.config.writer;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.*;

import java.util.*;
import org.apache.commons.logging.*;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutSigDataWriter implements ItemStreamWriter<MutSig> {

    @Autowired
    MutSigJdbcDaoImpl mutSigJdbcDaoImpl;
    
    private int mutSigDataCount;
    private int qValueRejects;
    
    private final Set<Integer> entrezGeneIdSet = new HashSet<>();
    
    private static final Log LOG = LogFactory.getLog(MutSigDataWriter.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        LOG.info("Beginning mutsig data import");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // add mutsig data count to execution context for step listener
        executionContext.put("mutSigDataCount", mutSigDataCount);
        executionContext.put("qValueRejects", qValueRejects);
        executionContext.put("totalGeneCount", entrezGeneIdSet.size());
    }

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends MutSig> list) throws Exception {
        List<MutSig> mutSigData = new ArrayList();
        for (MutSig mutSig : list) {
            // filter out high q values
            if (mutSig.getQValue() >= 0.1) {
                this.qValueRejects++;
                continue;
            }
            // update entrez gene id set and add mut sig record to list
            entrezGeneIdSet.add(mutSig.getEntrezGeneId());            
            mutSigData.add(mutSig);
        }
        // import batch of mutsig records and update mutsig data count (mutsig records imported)
        int rowsAffected = mutSigJdbcDaoImpl.addMutSigBatch(mutSigData);
        this.mutSigDataCount += rowsAffected;
    }
    
}

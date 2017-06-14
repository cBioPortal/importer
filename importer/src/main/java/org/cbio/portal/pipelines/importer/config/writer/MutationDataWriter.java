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
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.config.composite.CompositeMutationData;

import java.util.*;
import org.apache.commons.logging.*;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutationDataWriter implements ItemStreamWriter<CompositeMutationData> {

    @Autowired
    SampleProfileJdbcDaoImpl sampleProfileJdbcDaoImpl;
    
    @Autowired
    MutationJdbcDaoImpl mutationJdbcDaoImpl;
    
    private Integer nextMutationEventId;
    private MutationFilter mutationFilter;
    
    private boolean isMutationDatatype;
    private int mutationDataCount;
    private int mutationEventDataCount;
    private Integer genePanelId;
    
    private final Set<Integer> caseIdSet = new LinkedHashSet<>();
    private final Set<Integer> entrezGeneIdSet = new HashSet<>();
    
    private static final Log LOG = LogFactory.getLog(MutationDataWriter.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.mutationFilter = new MutationFilter();
        this.nextMutationEventId = mutationJdbcDaoImpl.getLargestMutationEventId();
        this.isMutationDatatype = (boolean) executionContext.get("isMutationDatatype");
        this.genePanelId = (Integer) executionContext.get("genePanelId");
        LOG.info("Beginning mutation data batch import");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // update mutation and mutation event data counts, total genes, and 
        // case list to execution context for step listener 
        executionContext.put("mutationDataCount", mutationDataCount);
        executionContext.put("mutationEventDataCount", mutationEventDataCount);
        executionContext.put("mutationFilter", mutationFilter);
        executionContext.put("totalGeneCount", entrezGeneIdSet.size());
        executionContext.put("caseList", caseIdSet);
    }

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends CompositeMutationData> list) throws Exception {        
        for (CompositeMutationData cmd : list) {
            if (isMutationDatatype && !mutationFilter.acceptMutation(cmd.getMutation())) {
                continue;
            }
            // add case id and gene to lists
            caseIdSet.add(cmd.getMutation().getSampleId());
            entrezGeneIdSet.add(cmd.getMutation().getEntrezGeneId());
            
            // add sample profile it not already exits for genetic profile
            if (!sampleProfileJdbcDaoImpl.existsInGeneticProfile(cmd.getMutation().getSampleId(), cmd.getMutation().getGeneticProfileId())) {
                sampleProfileJdbcDaoImpl.addSampleProfile(cmd.getMutation().getSampleId(), cmd.getMutation().getGeneticProfileId(), genePanelId);
            }
            
            // check if mutation event alreay exists
            MutationEvent existingMutationEvent = mutationJdbcDaoImpl.getMutationEvent(cmd.getMutation().getMutationEvent());
            if (existingMutationEvent != null) {
                // update mutation with existing mutation event
                cmd.updateMutationEvent(existingMutationEvent);
            }            
            else {
                // update mutation event id with next mutation event id
                cmd.setMutationEventId(++nextMutationEventId);
                mutationJdbcDaoImpl.addMutationEvent(cmd.getMutation().getMutationEvent());
                this.mutationEventDataCount++;
            }
            
            // check if mutation already exists
            Mutation existingMutation = mutationJdbcDaoImpl.getMutation(cmd.getMutation());
            if (existingMutation != null) {                
                // if exists then merge current mutation with existing mutation
                Mutation mergedMutation = MutationDataUtils.mergeMutationData(existingMutation, cmd.getMutation());
                mutationJdbcDaoImpl.updateMutation(mergedMutation);
            }
            else {
                mutationJdbcDaoImpl.addMutation(cmd.getMutation());
                this.mutationDataCount++;
            }
        }
    }

}

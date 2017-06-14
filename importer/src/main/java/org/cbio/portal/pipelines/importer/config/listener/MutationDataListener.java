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

package org.cbio.portal.pipelines.importer.config.listener;

import org.mskcc.cbio.model.GeneticProfile;
import org.mskcc.cbio.persistence.jdbc.MutationJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.util.MutationFilter;

import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class MutationDataListener implements StepExecutionListener {
        
    @Autowired
    MutationJdbcDaoImpl mutationJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(MutationDataListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add genetic profile, gene panel id, data file list, and mutation file metadata to execution context
        stepExecution.getExecutionContext().put("geneticProfile", stepExecution.getJobExecution().getExecutionContext().get("geneticProfile"));
        stepExecution.getExecutionContext().put("genePanelId", stepExecution.getJobExecution().getExecutionContext().get("genePanelId"));
        stepExecution.getExecutionContext().put("dataFileList", stepExecution.getJobExecution().getExecutionContext().get("dataFileList"));
        stepExecution.getExecutionContext().put("mutationFileMetadata", stepExecution.getJobExecution().getExecutionContext().get("mutationFileMetadata"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        GeneticProfile geneticProfile = (GeneticProfile) stepExecution.getJobExecution().getExecutionContext().get("geneticProfile");
        MultiKeyMap datatypeMetadata = (MultiKeyMap) stepExecution.getJobExecution().getExecutionContext().get("datatypeMetadata");
        String datatype = (String) stepExecution.getJobExecution().getExecutionContext().get("currentDatatype");
        String stepName = stepExecution.getStepName();

        // get counts for total samples and entries skipped for datatype
        int samplesSkipped = (int) stepExecution.getExecutionContext().get("samplesSkipped");
        int entriesSkipped = stepExecution.getFilterCount(); // filter count = # records returned as null from processor
        
        // get counts of total mutation records imported for datatype
        int mutationDataCount = (int) stepExecution.getExecutionContext().get("mutationDataCount");
        int mutationEventDataCount = (int) stepExecution.getExecutionContext().get("mutationEventDataCount");

        // check total count for records imported before calculating statistics
        if ((mutationDataCount + mutationEventDataCount) == 0) {
            LOG.error("Error importing mutation data for datatype: " + datatype);
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // if datatype is mutation datatype then print summary statistics for mutation filter
            // and calculate mutation count for every sample
            MutationFilter mutationFilter = (MutationFilter) stepExecution.getExecutionContext().get("mutationFilter");
            if (datatype.contains("mutation")) {
                LOG.info("Calculating mutation count for every sample for genetic profile: " + geneticProfile.getStableId());
                mutationJdbcDaoImpl.calculateMutationCount(geneticProfile.getGeneticProfileId());
                mutationFilter.printSummaryStatistics();
            }
            
            // get case list and total genes count
            Set<Integer> caseList = (Set<Integer>) stepExecution.getExecutionContext().get("caseList");
            int totalGenes = (int) stepExecution.getExecutionContext().get("totalGeneCount");
            
            // log counts during data loading and import
            LOG.info("Total samples loaded for datatype: " + caseList.size());
            LOG.info("Total genes loaded for datatype: " + totalGenes);
            LOG.info("Samples skipped during data loading: " + samplesSkipped);
            LOG.info("Entries skipped during data loading: " + entriesSkipped);
            LOG.info("Total records imported into MUTATION: " + mutationDataCount);
            LOG.info("Total records imported into MUTATION_EVENT: " + mutationEventDataCount);
            

            
            // update the case list for datatype metadata and update execution context
            datatypeMetadata.put(datatype, "caseList", caseList);
            stepExecution.getJobExecution().getExecutionContext().put("datatypeMetadata", datatypeMetadata);
        }

        // log rollbacks and number of items skipped during current step execution
        if (stepExecution.getRollbackCount() > 0) {
            LOG.info("Rollbacks during " + stepName + ": " + stepExecution.getRollbackCount());
        }
        if (stepExecution.getSkipCount() > 0) {
            LOG.info("Items skipped " + stepName + ": " + stepExecution.getSkipCount());
        }
                
        return ExitStatus.COMPLETED;
    }
    
}

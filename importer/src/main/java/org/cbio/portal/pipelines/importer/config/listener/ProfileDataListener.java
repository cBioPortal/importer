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

import java.util.*;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;
import org.springframework.batch.core.*;

/**
 *
 * @author ochoaa
 */
public class ProfileDataListener implements StepExecutionListener {

    private String datatype;
    private boolean isRppaProfile;
    private boolean isCnaData;
    
    private static final Log LOG = LogFactory.getLog(ProfileDataListener.class);
        
    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.datatype = (String) stepExecution.getJobExecution().getExecutionContext().get("currentDatatype");
        
        // add genetic profile and data file list to execution context
        stepExecution.getExecutionContext().put("geneticProfile", stepExecution.getJobExecution().getExecutionContext().get("geneticProfile"));
        stepExecution.getExecutionContext().put("dataFileList", stepExecution.getJobExecution().getExecutionContext().get("dataFileList"));
        stepExecution.getExecutionContext().put("profileMetadata", stepExecution.getJobExecution().getExecutionContext().get("profileMetadata"));
        
        // set booleans for determing if datatype is RPPA profile or CNA data
        this.isRppaProfile = datatype.startsWith("rppa");
        this.isCnaData = datatype.contains("cna");
        stepExecution.getExecutionContext().put("isRppaProfile", isRppaProfile);
        stepExecution.getExecutionContext().put("isCnaData", isCnaData);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        MultiKeyMap datatypeMetadata = (MultiKeyMap) stepExecution.getJobExecution().getExecutionContext().get("datatypeMetadata");
        String stepName = stepExecution.getStepName();

        // get counts for total samples and genes loaded for datatype
        int totalGenes = (int) stepExecution.getExecutionContext().get("totalGeneCount");
        int totalGeneticAlterationCount = (int) stepExecution.getExecutionContext().get("geneticAlterationCount");

        // get counts for total samples and entries skipped for datatype
        int samplesSkipped = (int) stepExecution.getExecutionContext().get("samplesSkipped");
        int additionalEntriesSkipped = (int) stepExecution.getExecutionContext().get("additionalEntriesSkipped");
        int entriesSkipped = additionalEntriesSkipped + stepExecution.getFilterCount(); // filter count = # records returned as null from processor
        
        // get RPPA profile status and counts of extra records
        boolean isRppaProfile = (boolean) stepExecution.getExecutionContext().get("isRppaProfile");
        int validExtraRecords = (int) stepExecution.getExecutionContext().get("validExtraRecords");
        int skippedExtraRecords = (int) stepExecution.getExecutionContext().get("skippedExtraRecords");
        
        if (totalGeneticAlterationCount == 0) {
            LOG.error("Error importing profile data for datatype: " + datatype);
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // get case list
            Set<Integer> caseList = (Set<Integer>) stepExecution.getExecutionContext().get("caseList");
            
            // log counts during data loading and import
            LOG.info("Total samples loaded for datatype: " + caseList.size());
            LOG.info("Total genes loaded for datatype: " + totalGenes);
            LOG.info("Normal samples skipped during data loading: " + samplesSkipped);
            LOG.info("Total entries skipped during data loading: " + entriesSkipped);
            
            LOG.info("Total records imported into GENETIC_ALTERATION: " + totalGeneticAlterationCount);
            if (isCnaData) {
                LOG.info("Total records imported into CNA_EVENT: " + stepExecution.getExecutionContext().get("cnaEventCount"));
                LOG.info("Total records imported into SAMPLE_CNA_EVENT: " + stepExecution.getExecutionContext().get("sampleCnaEventCount"));
            }
            
            // log counts of extra records loaded during data loading
            if (isRppaProfile && validExtraRecords > 0) {
                LOG.info("Total number of extra records added because of multiple genes in one line: " + validExtraRecords);
            }
            if (skippedExtraRecords > 0) {
                LOG.info("Total number of extra records skipped because of ambiguous gene symbols: " + skippedExtraRecords);
            }
            
            // update the case list for datatype metadata add update execution context
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

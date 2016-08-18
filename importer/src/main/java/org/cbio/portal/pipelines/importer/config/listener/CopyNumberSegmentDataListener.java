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
public class CopyNumberSegmentDataListener implements StepExecutionListener {
        
    private static final Log LOG = LogFactory.getLog(CopyNumberSegmentDataListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add cancer study, datafile list, and seg file metadata to step execution context
        stepExecution.getExecutionContext().put("cancerStudy", stepExecution.getJobExecution().getExecutionContext().get("cancerStudy"));
        stepExecution.getExecutionContext().put("dataFile", stepExecution.getJobExecution().getExecutionContext().get("dataFile"));
        stepExecution.getExecutionContext().put("copyNumberSegmentMetadata", stepExecution.getJobExecution().getExecutionContext().get("copyNumberSegmentMetadata"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        MultiKeyMap datatypeMetadata = (MultiKeyMap) stepExecution.getJobExecution().getExecutionContext().get("datatypeMetadata");
        String datatype = (String) stepExecution.getJobExecution().getExecutionContext().get("currentDatatype");
        String stepName = stepExecution.getStepName();
        
        // get number of samples skipped, entries skipped, and total records imported
        int samplesSkipped = (int) stepExecution.getExecutionContext().get("samplesSkipped");
        int entriesSkipped = stepExecution.getFilterCount(); // filter count = # records returned as null from processor
        int copyNumberSegmentDataCount = (int) stepExecution.getExecutionContext().get("copyNumberSegmentDataCount");
        
        if (copyNumberSegmentDataCount == 0) {
            LOG.error("Error importing copy number segment data for datatype: " + datatype);
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // get case list
            Set<Integer> caseList = (Set<Integer>) stepExecution.getExecutionContext().get("caseList");
            
            // log counts during data loading and importing
            LOG.info("Total samples loaded for datatype: " + caseList.size());
            LOG.info("Normal samples skipped during data loading: " + samplesSkipped);
            LOG.info("Total entries skipped during data loading: " + entriesSkipped);
            LOG.info("Total records imported into COPY_NUMBER_SEG: " + copyNumberSegmentDataCount);
            
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

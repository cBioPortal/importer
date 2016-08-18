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

import org.apache.commons.logging.*;
import org.springframework.batch.core.*;

/**
 *
 * @author ochoaa
 */
public class TimelineDataListener implements StepExecutionListener {

    private static final Log LOG = LogFactory.getLog(TimelineDataListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add cancer study, data file list, and timeline file metadata from tasklet to step execution context
        stepExecution.getExecutionContext().put("cancerStudy", stepExecution.getJobExecution().getExecutionContext().get("cancerStudy"));
        stepExecution.getExecutionContext().put("dataFileList", stepExecution.getJobExecution().getExecutionContext().get("dataFileList"));
        stepExecution.getExecutionContext().put("timelineMetadata", stepExecution.getJobExecution().getExecutionContext().get("timelineMetadata"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        String stepName = stepExecution.getStepName();
        
        // get number of patients skipped and total records imported
        int patientsSkipped = (int) stepExecution.getExecutionContext().get("patientsSkipped");
        int clinicalEventCount = (int) stepExecution.getExecutionContext().get("clinicalEventCount");
        int clinicalEventDataCount = (int) stepExecution.getExecutionContext().get("clinicalEventDataCount");
        
        if ((clinicalEventCount + clinicalEventDataCount) == 0) {
            LOG.error("No records were imported into CLINICAL_EVENT or CLINICAL_EVENT_DATA");
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // log counts during data loading and importing
            LOG.info("Total patients skipped during data loading: " + patientsSkipped);
            LOG.info("Total records imported into CLINICAL_EVENT: " + clinicalEventCount);
            LOG.info("Total records imported into CLINICAL_EVENT_DATA: " + clinicalEventDataCount);
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

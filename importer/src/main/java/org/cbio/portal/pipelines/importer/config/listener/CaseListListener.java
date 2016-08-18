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
public class CaseListListener implements StepExecutionListener {

    private static final Log LOG = LogFactory.getLog(CaseListListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add cancer study and datatype metadata to step execution context
        stepExecution.getExecutionContext().put("cancerStudy", stepExecution.getJobExecution().getExecutionContext().get("cancerStudy"));
        stepExecution.getExecutionContext().put("datatypeMetadata", stepExecution.getJobExecution().getExecutionContext().get("datatypeMetadata"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        String stepName = stepExecution.getStepName();
        
        // get counts for sample list and sample list list
        int sampleListDataCount = (int) stepExecution.getExecutionContext().get("sampleListDataCount");
        int sampleListListDataCount = (int) stepExecution.getExecutionContext().get("sampleListListDataCount");
        
        if (sampleListDataCount == 0 && sampleListListDataCount == 0) {
            LOG.error("Error import sample list records into SAMPLE_LIST and SAMPLE_LIST_LIST");
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // log the sample list and sample list list record counts
            LOG.info("Sample list records imported: " + sampleListDataCount);
            LOG.info("Sample list list records imported: " + sampleListListDataCount);
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

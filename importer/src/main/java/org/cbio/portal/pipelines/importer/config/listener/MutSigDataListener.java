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
public class MutSigDataListener implements StepExecutionListener {
    
    private static final Log LOG = LogFactory.getLog(MutSigDataListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add cancer study, datafile, and mutsig file metadata from tasklet to step execution context
        stepExecution.getExecutionContext().put("cancerStudy", stepExecution.getJobExecution().getExecutionContext().get("cancerStudy"));
        stepExecution.getExecutionContext().put("dataFile", stepExecution.getJobExecution().getExecutionContext().get("dataFile"));
        stepExecution.getExecutionContext().put("mutSigMetadata", stepExecution.getJobExecution().getExecutionContext().get("mutSigMetadata"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        String stepName = stepExecution.getStepName();
        
        // get total gene count, high q-value rejects, number of entries skipped, and total records imported        
        int totalGenes = (int) stepExecution.getExecutionContext().get("totalGeneCount");
        int qValueRejects = (int) stepExecution.getExecutionContext().get("qValueRejects");
        int entriesSkipped = stepExecution.getFilterCount(); // filter count = # records returned as null from processor
        int mutSigDataCount = (int) stepExecution.getExecutionContext().get("mutSigDataCount");
        
        if (mutSigDataCount == 0) {
            LOG.error("No records were imported into MUT_SIG - " + 
                    (totalGenes==0?"data could not be loaded from file":("expected number of records: " + totalGenes)));
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // log counts during data loading and importing
            LOG.info("Total genes loaded from mutsig file: " + totalGenes);
            LOG.info("Total records skipped because of q-value >= 0.1 during data loading: " + qValueRejects);
            LOG.info("Total entries skipped during data loading: " + entriesSkipped);
            LOG.info("Total records imported into MUT_SIG: " + mutSigDataCount);
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

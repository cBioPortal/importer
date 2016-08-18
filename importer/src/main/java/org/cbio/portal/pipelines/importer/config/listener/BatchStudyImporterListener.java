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

import org.mskcc.cbio.model.CancerStudy;
import org.mskcc.cbio.persistence.jdbc.CancerStudyJdbcDaoImpl;

import org.apache.commons.logging.*;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Listener for batch study importer job execution.
 * 
 * @author ochoaa
 */
public class BatchStudyImporterListener implements JobExecutionListener {

    @Autowired
    CancerStudyJdbcDaoImpl cancerStudyJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(BatchStudyImporterListener.class);   
    
    @Override
    public void beforeJob(JobExecution jobExecution) {}
    
    @Override
    public void afterJob(JobExecution jobExecution) {
        CancerStudy cancerStudy = (CancerStudy) jobExecution.getExecutionContext().get("cancerStudy");
        boolean rollbackCancerStudyState = (boolean) jobExecution.getExecutionContext().get("rollbackCancerStudyState");        

        // delete cancer study if rollback cancer study state is true or if exit status not COMPLETED
        if (jobExecution.getExitStatus().equals(ExitStatus.COMPLETED) && !rollbackCancerStudyState) {
            LOG.info("Import complete for cancer study: " + cancerStudy.getCancerStudyIdentifier());
        }
        else {
            LOG.error("Job STOPPED or FAILED for study: " + cancerStudy.getCancerStudyIdentifier());
            if (rollbackCancerStudyState) {
                LOG.error("Cancer study import contains errors - deleting imported data for study: " + cancerStudy.getCancerStudyIdentifier());
            }
            cancerStudyJdbcDaoImpl.deleteCancerStudy(cancerStudy.getCancerStudyId());
        }
    }
    
}

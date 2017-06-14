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
public class ClinicalDataListener implements StepExecutionListener {

    private static final Log LOG = LogFactory.getLog(ClinicalDataListener.class);
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        // add cancer study, datafile list, and clinical file metadata to step execution context
        stepExecution.getExecutionContext().put("cancerStudy", stepExecution.getJobExecution().getExecutionContext().get("cancerStudy"));
        stepExecution.getExecutionContext().put("clinicalMetadata", stepExecution.getJobExecution().getExecutionContext().get("clinicalMetadata"));
        stepExecution.getExecutionContext().put("dataFileList", stepExecution.getJobExecution().getExecutionContext().get("dataFileList"));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        MultiKeyMap datatypeMetadata = (MultiKeyMap) stepExecution.getJobExecution().getExecutionContext().get("datatypeMetadata");
        String datatype = (String) stepExecution.getJobExecution().getExecutionContext().get("currentDatatype");
        String stepName = stepExecution.getStepName();

        // log new attributes count
        int newClinicalAttributes = stepExecution.getJobExecution().getExecutionContext().getInt("newClinicalAttributes", 0);
        if (newClinicalAttributes > 0) {
            LOG.info("New clinical attributes imported: " + newClinicalAttributes);
        }        
        
        // get counts for patient, sample, and clinical data records inserted
        int patientCount = (int) stepExecution.getExecutionContext().get("patientCount");
        int sampleCount = (int) stepExecution.getExecutionContext().get("sampleCount");
        int patientDataCount = (int) stepExecution.getExecutionContext().get("patientDataCount");
        int sampleDataCount = (int) stepExecution.getExecutionContext().get("sampleDataCount");
        
        int totalCount = Arrays.stream(new int[]{patientCount, sampleCount, patientDataCount, sampleDataCount}).sum();
        if (totalCount == 0) {
            LOG.error("No records were imported for datatype: " + datatype);
            // change rollback state for cancer study to true to return to saved state
            stepExecution.getJobExecution().getExecutionContext().put("rollbackCancerStudyState", true);
        }
        else {
            // log the record counts
            LOG.info("Patient records imported: " + patientCount);
            LOG.info("Patient clinical data records imported: " + patientDataCount);
            LOG.info("Sample records imported: " + sampleCount);
            LOG.info("Sample clinical data records imported: " + sampleDataCount);
            
            // update the case list for datatype metadata add update execution context
            datatypeMetadata.put(datatype, "caseList", stepExecution.getExecutionContext().get("caseList"));
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

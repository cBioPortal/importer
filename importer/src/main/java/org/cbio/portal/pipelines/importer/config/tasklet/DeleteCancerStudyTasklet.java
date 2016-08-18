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

package org.cbio.portal.pipelines.importer.config.tasklet;

import org.mskcc.cbio.model.CancerStudy;
import org.mskcc.cbio.persistence.jdbc.CancerStudyJdbcDaoImpl;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Tasklet for deleting a cancer study given a cancer study identifier.
 * 
 * @author ochoaa
 */
public class DeleteCancerStudyTasklet implements Tasklet {
    
    @Value("#{jobParameters[cancerStudyIdentifier]}")
    private String cancerStudyIdentifier;
    
    @Autowired
    CancerStudyJdbcDaoImpl cancerStudyJdbcDaoImpl;
    
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        CancerStudy cancerStudy = cancerStudyJdbcDaoImpl.getCancerStudy(cancerStudyIdentifier);
        if (cancerStudy != null) {
            cancerStudyJdbcDaoImpl.deleteCancerStudy(cancerStudy.getCancerStudyId());
        }
        return RepeatStatus.FINISHED;
    }
    
}

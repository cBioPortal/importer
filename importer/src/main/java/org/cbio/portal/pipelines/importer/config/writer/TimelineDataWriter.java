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

import java.util.*;
import org.apache.commons.logging.*;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class TimelineDataWriter implements ItemStreamWriter<ClinicalEvent> {
    
    @Autowired
    ClinicalEventJdbcDaoImpl clinicalEventJdbcDaoImpl;
    
    private Integer nextClinicalEventId;
    
    private int clinicalEventCount;
    private int clinicalEventDataCount;
    private final Set<Integer> caseIdSet = new LinkedHashSet<>();
    
    private static final Log LOG = LogFactory.getLog(TimelineDataWriter.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.nextClinicalEventId = clinicalEventJdbcDaoImpl.getLargestClinicalEventId();
        LOG.info("Beginning clinical event data batch import");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // update clinical event and clinical event data counts for step listener
        executionContext.put("clinicalEventCount", clinicalEventCount);
        executionContext.put("clinicalEventDataCount", clinicalEventDataCount);
        executionContext.put("caseList", caseIdSet);
    }

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends ClinicalEvent> list) throws Exception {
        List<ClinicalEvent> clinicalEventList = new ArrayList();
        List<ClinicalEventData> clinicalEventDataList = new ArrayList();
        
        // go through list and add all clinical events and clinical event data
        // to lists for import
        for (ClinicalEvent clinicalEvent : list) {
            // add case id to case id set 
            caseIdSet.add(clinicalEvent.getPatientId());
            
            // upate clinical event id for clinical event
            clinicalEvent.setClinicalEventId(++nextClinicalEventId);
            clinicalEventList.add(clinicalEvent);
            
            // only update clinical event id for clinical event data list if not empty 
            if (clinicalEvent.getClinicalEventData().isEmpty()) {
                continue;
            }
            clinicalEvent.getClinicalEventData().forEach((ced) -> {
                ced.setClinicalEventId(nextClinicalEventId);
            });
            clinicalEventDataList.addAll(clinicalEvent.getClinicalEventData());
        }
        
        // import clinical events and clinical event data to each table if not empty
        if (!clinicalEventList.isEmpty()) {
            int rowsAffected = clinicalEventJdbcDaoImpl.addClinicalEventBatch(clinicalEventList);
            this.clinicalEventCount += rowsAffected;
        }
        if (!clinicalEventDataList.isEmpty()) {
            int rowsAffected = clinicalEventJdbcDaoImpl.addClinicalEventDataBatch(clinicalEventDataList);
            this.clinicalEventDataCount += rowsAffected;
        }
    }
    
}

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

package org.cbio.portal.pipelines.importer.config.processor;

import org.mskcc.cbio.model.*;
import org.cbio.portal.pipelines.importer.model.*;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;

import java.util.*;
import org.springframework.batch.item.ItemProcessor;

/**
 *
 * @author ochoaa
 */
public class TimelineDataProcessor implements ItemProcessor<TimelineRecord, ClinicalEvent> {

    @Override
    public ClinicalEvent process(TimelineRecord timelineRecord) throws Exception {
        // extract the clinical event and transform list of clinical event data from timeline record
        ClinicalEvent clinicalEvent = transformTimelineRecordToClinicalEvent(timelineRecord);
        List<ClinicalEventData> clinicalEventDataList = extractClinicalEventDataMap(timelineRecord.getClinicalEventDataMap());
        clinicalEvent.setClinicalEventData(clinicalEventDataList);
        
        return clinicalEvent;
    }
    
    /**
     * Transform timeline record into a clinical event instance.
     * 
     * @param timelineRecord
     * @return ClinicalEvent
     */
    private ClinicalEvent transformTimelineRecordToClinicalEvent(TimelineRecord timelineRecord) {
        ClinicalEvent clinicalEvent = new ClinicalEvent();
        clinicalEvent.setPatientId(timelineRecord.getPatientInternalId());
        clinicalEvent.setStartDate(Integer.valueOf(timelineRecord.getStartDate()));
        clinicalEvent.setEventType(timelineRecord.getEventType());
        
        // only set stop date if value not null or empty in file
        if (!DataFileUtils.isNullOrEmptyValue(timelineRecord.getStartDate())) {
            clinicalEvent.setStopDate(Integer.valueOf(timelineRecord.getStartDate()));
        }

        return clinicalEvent;
    }
    
    /**
     * Extract clinical data event map from timeline record to list. 
     * 
     * @param clinicalEventDataMap
     * @return List<ClinicalEventData>
     */
    private List<ClinicalEventData> extractClinicalEventDataMap(Map<String, String> clinicalEventDataMap) {
        List<ClinicalEventData> clinicalEventDataList = new ArrayList();
        for (String key : clinicalEventDataMap.keySet()) {
            ClinicalEventData clinicalDataEvent = new ClinicalEventData();
            clinicalDataEvent.setKey(key);
            clinicalDataEvent.setValue(clinicalEventDataMap.get(key));
            clinicalEventDataList.add(clinicalDataEvent);
        }
        
        return clinicalEventDataList;
    }
    
}

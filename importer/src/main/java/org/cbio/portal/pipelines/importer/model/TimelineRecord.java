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

package org.cbio.portal.pipelines.importer.model;

import java.util.*;

/**
 *
 * @author ochoaa
 */
public class TimelineRecord {
    
    private Integer patientInternalId;
    private String patientId;
    private String startDate;
    private String stopDate;
    private String eventType;
    private Map<String, String> clinicalEventDataMap;

    /**
     * @return the patientInternalId
     */
    public Integer getPatientInternalId() {
        return patientInternalId;
    }

    /**
     * @param patientInternalId the patientInternalId to set
     */
    public void setPatientInternalId(Integer patientInternalId) {
        this.patientInternalId = patientInternalId;
    }

    /**
     * @return the patientId
     */
    public String getPatientId() {
        return patientId;
    }

    /**
     * @param patientId the patientId to set
     */
    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    /**
     * @return the startDate
     */
    public String getStartDate() {
        return startDate;
    }

    /**
     * @param startDate the startDate to set
     */
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    /**
     * @return the stopDate
     */
    public String getStopDate() {
        return stopDate;
    }

    /**
     * @param stopDate the stopDate to set
     */
    public void setStopDate(String stopDate) {
        this.stopDate = stopDate;
    }

    /**
     * @return the eventType
     */
    public String getEventType() {
        return eventType;
    }

    /**
     * @param eventType the eventType to set
     */
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    /**
     * @return the clinicalEventDataMap
     */
    public Map<String, String> getClinicalEventDataMap() {
        return clinicalEventDataMap;
    }

    /**
     * @param clinicalEventDataMap the clinicalEventDataMap to set
     */
    public void setClinicalEventDataMap(Map<String, String> clinicalEventDataMap) {
        this.clinicalEventDataMap = clinicalEventDataMap;
    }

    /**
     * @return the timeline staging data map (column -> field)
     */
    public Map<String, String> getTimelineStagingDataMap() {
        Map<String, String> map = new HashMap<>();
        map.put("PATIENT_ID", "patientId");
        map.put("START_DATE", "startDate");
        map.put("STOP_DATE", "stopDate");
        map.put("EVENT_TYPE", "eventType");
        
        return map;
    }
    
}

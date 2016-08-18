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

import org.mskcc.cbio.model.CopyNumberSegment;
import org.cbio.portal.pipelines.importer.util.*;
import org.cbio.portal.pipelines.importer.model.CopyNumberSegmentRecord;

import com.google.common.base.Strings;
import org.apache.commons.logging.*;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class CopyNumberSegmentDataProcessor implements ItemProcessor<CopyNumberSegmentRecord, CopyNumberSegment> {
    
    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(CopyNumberSegmentDataProcessor.class);
    
    @Override
    public CopyNumberSegment process(CopyNumberSegmentRecord copyNumberSegmentRecord) throws Exception {
        CopyNumberSegment screenedRecord = screenCopyNumberSegmentRecord(copyNumberSegmentRecord);

        return screenedRecord;
    }
    
    /**
     * Performs basic data screening to determine whether copy number segment record is acceptable or not.
     * Returns null if copy number segment record does not pass screening
     * 
     * @param copyNumberSegmentRecord
     * @return CopyNumberSegment
     */
    private CopyNumberSegment screenCopyNumberSegmentRecord(CopyNumberSegmentRecord copyNumberSegmentRecord) {
        // check normalized chromosome value
        String normalizedChromosome = geneDataUtils.getNormalizedChromosome(copyNumberSegmentRecord.getChrom());
        if (Strings.isNullOrEmpty(normalizedChromosome)) {            
            LOG.warn("Skipping entry with chromosome value: " + copyNumberSegmentRecord.getChrom());
            return null;
        }

        // check start and end locations
        Integer locStart = !DataFileUtils.isNullOrEmptyValue(copyNumberSegmentRecord.getLocStart())?
                Integer.valueOf(copyNumberSegmentRecord.getLocStart()):-1;
        Integer locEnd = !DataFileUtils.isNullOrEmptyValue(copyNumberSegmentRecord.getLocEnd())?
                Integer.valueOf(copyNumberSegmentRecord.getLocEnd()):-1;
        if (locStart >= locEnd) {
            LOG.warn("Skipping entry with start location >= end location: " + locStart + " >= " + locEnd);                    
            return null;
        }
        
        // create new CopyNumberSegment instance with processed/resolved data from above
        CopyNumberSegment copyNumberSegment = new CopyNumberSegment();
        copyNumberSegment.setChr(normalizedChromosome);
        copyNumberSegment.setStart(locStart);
        copyNumberSegment.setEnd(locEnd);
        copyNumberSegment.setNumProbes(Integer.valueOf(copyNumberSegmentRecord.getNumProbes()));
        copyNumberSegment.setSegmentMean(Double.valueOf(copyNumberSegmentRecord.getSegMean()));
        
        return copyNumberSegment;
    }
    
}

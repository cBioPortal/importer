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


import java.util.*;
import com.google.common.base.Strings;
import org.apache.commons.logging.*;
import org.cbio.portal.pipelines.importer.model.GisticRecord;
import org.cbio.portal.pipelines.importer.util.GeneDataUtils;
import org.mskcc.cbio.model.*;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class GisticDataProcessor implements ItemProcessor<GisticRecord, Gistic> {

    @Autowired
    GeneDataUtils geneDataUtils;
    
    private static final Log LOG = LogFactory.getLog(GisticDataProcessor.class);
    
    @Override
    public Gistic process(GisticRecord gisticRecord) throws Exception {
        Gistic screenedRecord = screenGisticRecord(gisticRecord);
        
        return screenedRecord;
    }
    
    /**
     * Performs basic data screening to determine whether gistic record is acceptable or not.
     * Returns null if gistic record does not pass screening
     * 
     * @param gisticRecord
     * @return Gistic
     */
    private Gistic screenGisticRecord(GisticRecord gisticRecord) {
        // check that normalized chromosome is valid
        String normalizedChromosome = geneDataUtils.getNormalizedChromosome(gisticRecord.getChromosome());
        if (Strings.isNullOrEmpty(normalizedChromosome)) {
            LOG.warn("Skipping entry with chromosome value: " + gisticRecord.getChromosome());
            return null;
        }
                
        // go through list of genes - ignore genes that have already been processed and ignore miRNA's
        List<GisticToGene> gisticToGeneList = new ArrayList();
        Set<Integer> genesAdded = new HashSet<>();
        String[] genesInRegion = gisticRecord.getGenesInRegion().replaceAll("\\[|\\]", "").split(",");
        for (String geneSymbol : genesInRegion) {
            geneSymbol = geneSymbol.split("\\|")[0];
            Gene gene = geneDataUtils.resolveGeneFromRecordData(geneSymbol, normalizedChromosome);
            if (gene == null) {
                LOG.warn("Skipping ambiguous gene: " + geneSymbol);
                continue;
            }

            // skip miRNA's 
            if (gene.getType().equals(geneDataUtils.MIRNA_TYPE) ) {
                LOG.warn("Ignoring miRNA: " + gene.getHugoGeneSymbol());
                continue;
            }
            
            // skip genes that already have data loaded for them
            if (genesAdded.contains(gene.getEntrezGeneId())) {
                LOG.warn("Skipping gene that has been given as alias in your file: " + gene.getHugoGeneSymbol());
                continue;
            }
            
            // add gistic gene to list
            genesAdded.add(gene.getEntrezGeneId());
            GisticToGene gisticGene = new GisticToGene();
            gisticGene.setEntrezGeneId(gene.getEntrezGeneId());
            gisticToGeneList.add(gisticGene);
        }

        // skip entry if genes could not be found in db
        if (gisticToGeneList.isEmpty()) {
            LOG.warn("Genes could not be found for entry - skipping gistic event");
            return null;
        }        
        // create new Gistic instance with processed/resolved data from above        
        Gistic gistic = new Gistic();
        gistic.setCancerStudyId(gisticRecord.getCancerStudyId());
        gistic.setChromosome(Integer.valueOf(normalizedChromosome));
        gistic.setCytoband(gisticRecord.getCytoband());
        gistic.setWidePeakStart(Integer.valueOf(gisticRecord.getPeakStart()));
        gistic.setWidePeakEnd(Integer.valueOf(gisticRecord.getPeakEnd()));
        gistic.setqValue(Double.valueOf(gisticRecord.getqValue()));
        gistic.setAmp(gisticRecord.getAmp().equals("1"));
        gistic.setGenesInRegion(gisticToGeneList);
        
        return gistic;        
    }
}

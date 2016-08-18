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
public class GisticDataWriter implements ItemStreamWriter<Gistic> {

    @Autowired
    GisticJdbcDaoImpl gisticJdbcDaoImpl;
    
    private Integer nextGisticRoiId;
    
    private int gisticDataCount;
    private int gisticGeneDataCount;
    
    private final Set<Integer> entrezGeneIdSet = new HashSet<>();    
    
    private static final Log LOG = LogFactory.getLog(GisticDataWriter.class);
    
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.nextGisticRoiId = gisticJdbcDaoImpl.getLargestGisticRoiId();
        LOG.info("Beginning gistic data batch import");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // update gistic data count and total genes for step listener
        executionContext.put("gisticDataCount", gisticDataCount);
        executionContext.put("gisticGeneDataCount", gisticGeneDataCount);
        executionContext.put("totalGeneCount", entrezGeneIdSet.size());
    }

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Gistic> list) throws Exception {
        List<Gistic> gisticList = new ArrayList();
        List<GisticToGene> gisticGeneList = new ArrayList();
        
        // go through list of gistic data and update gistic roi id
        for (Gistic gistic : list) {
            // update entrez gene id set
            gistic.getGenesInRegion().stream().forEach((gene) -> {
                entrezGeneIdSet.add(gene.getEntrezGeneId());
            });
            // update gistic roi id
            gistic.setGisticRoiId(++nextGisticRoiId);
            gisticList.add(gistic);
            
            // update gistic roi id for gistic gene list also
            gistic.getGenesInRegion().forEach((gg) -> {
                gg.setGisticRoiId(nextGisticRoiId);
            });
            gisticGeneList.addAll(gistic.getGenesInRegion());
        }
        
        // import gistic data and gistic genes to each table if not empty
        if (!gisticList.isEmpty()) {
            int rowsAffected = gisticJdbcDaoImpl.addGisticBatch(gisticList);
            this.gisticDataCount += rowsAffected;
        }
        if (!gisticGeneList.isEmpty()) {
            int rowsAffected = gisticJdbcDaoImpl.addGisticGenesBatch(gisticGeneList);
            this.gisticGeneDataCount += rowsAffected;
        }
    }
    
}

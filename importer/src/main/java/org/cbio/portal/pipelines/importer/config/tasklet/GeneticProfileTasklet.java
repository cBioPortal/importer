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

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.*;

import java.util.*;
import com.google.common.base.Strings;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.logging.*;

import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.*;

/**
 * Tasklet for loading and importing genetic profile data.
 * 
 * @author ochoaa
 */
public class GeneticProfileTasklet implements Tasklet {
    
    @Autowired
    GeneticProfileJdbcDaoImpl geneticProfileJdbcDaoImpl;
    
    @Autowired
    CancerStudyJdbcDaoImpl cancerStudyJdbcDaoImpl;
    
    @Autowired
    GenePanelJdbcDaoImpl genePanelJdbcDaoImpl;
    
    private String genePanel = null;
    private CancerStudy cancerStudy;
    
    private static final Log LOG = LogFactory.getLog(GeneticProfileTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        // get cancer study, datatype, and metafile properties from datatype metadata in execution context
        this.cancerStudy = (CancerStudy) chunkContext.getStepContext().getJobExecutionContext().get("cancerStudy");
        MultiKeyMap datatypeMetadata = (MultiKeyMap) chunkContext.getStepContext().getJobExecutionContext().get("datatypeMetadata");
        String datatype = (String) chunkContext.getStepContext().getJobExecutionContext().get("currentDatatype");
        Properties properties = (Properties) datatypeMetadata.get(datatype, "properties");

        // load genetic profile and insert genetic profile and data file list into execution context if not null
        GeneticProfile geneticProfile = loadGeneticProfileMetadata(properties);
        if (geneticProfile == null) {
            LOG.error("Error loading genetic profile from: " + datatypeMetadata.get(datatype, "meta_filename"));
            chunkContext.getStepContext().getStepExecution().getJobExecution().setStatus(BatchStatus.STOPPING);
            return RepeatStatus.FINISHED;
        }
        
        // check for existing genetic profile and stop job if genetic profile already exists except for datatype MAF
        GeneticProfile existingGeneticProfile = geneticProfileJdbcDaoImpl.getGeneticProfile(geneticProfile.getStableId());
        if (existingGeneticProfile != null) {
            if (!existingGeneticProfile.getDatatype().equals("MAF")) {
                LOG.error("Genetic profile already exists with stable id: " + geneticProfile.getStableId());
                chunkContext.getStepContext().getStepExecution().getJobExecution().setStatus(BatchStatus.STOPPING);
                return RepeatStatus.FINISHED;
            }
            // add existing genetic profile to execution context
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("geneticProfile", existingGeneticProfile);
        }
        else {
            // import new genetic profile and add to execution context
            GeneticProfile newGeneticProfile = geneticProfileJdbcDaoImpl.addGeneticProfile(geneticProfile);
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("geneticProfile", newGeneticProfile);
        }
        
        // add gene panel id to execution context - right now it is assumed that a gene panel, if given in a genetic profile file,
        // already exists in the db
        Integer genePanelId = null;
        if (!Strings.isNullOrEmpty(genePanel)) {
            genePanelId = genePanelJdbcDaoImpl.getGenePanelId(genePanel);
        }
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("genePanelId", genePanelId);
        
        return RepeatStatus.FINISHED;
    }
    
    /**
     * Load genetic profile metadata from meta file properties.
     * 
     * @param properties
     * @return GeneticProfile
     */
    private GeneticProfile loadGeneticProfileMetadata(Properties properties) {
        GeneticProfile geneticProfile = new GeneticProfile();
        
        // get cancer study by cancer study identifier
        geneticProfile.setCancerStudy(cancerStudy);
        geneticProfile.setCancerStudyId(cancerStudy.getCancerStudyId());
        
        // get stable id from meta file and insert cancer study identifier if necessary
        String stableId = properties.getProperty("stable_id");
        if (!stableId.startsWith(cancerStudy.getCancerStudyIdentifier()) ) {
            stableId = cancerStudy.getCancerStudyIdentifier() + "_" + stableId;
        }
        geneticProfile.setStableId(stableId);
        
        // set remaining genetic profile properties - default values are empty strings
        String geneticAlterationType = "";
        String profileName = "";
        String description = "";
        String datatype = "";
        String showProfileInAnalysisTab = "false";
        try {
            geneticAlterationType = properties.getProperty("genetic_alteration_type");
            profileName = properties.getProperty("profile_name");
            description = properties.getProperty("profile_description");
            datatype = properties.getProperty("datatype");
            showProfileInAnalysisTab = properties.getProperty("show_profile_in_analysis_tab", "false");
        }
        catch (NullPointerException ex) {}
        
        // set gene panel stable id if exists
        if (properties.containsKey("gene_panel")) {
            this.genePanel = properties.getProperty("gene_panel");
        }
        // set genetic profile properties 
        geneticProfile.setGeneticAlterationType(geneticAlterationType);
        geneticProfile.setName(profileName);
        geneticProfile.setDescription(description);
        geneticProfile.setDatatype(datatype);
        geneticProfile.setShowProfileInAnalysisTab(showProfileInAnalysisTab.equalsIgnoreCase("true"));

        return geneticProfile;
    }
    
}

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

package org.cbio.portal.pipelines.importer.config.reader;

import org.mskcc.cbio.model.*;
import org.mskcc.cbio.persistence.jdbc.SampleJdbcDaoImpl;
import org.cbio.portal.pipelines.importer.util.DataFileUtils;

import java.io.*;
import java.util.*;
import javax.annotation.Resource;
import org.apache.commons.logging.*;
import com.google.common.collect.Sets;
import org.apache.commons.collections.map.MultiKeyMap;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class CaseListReader implements ItemStreamReader<SampleList> {
    
    @Autowired
    SampleJdbcDaoImpl sampleJdbcDaoImpl;
    
    @Value("#{jobParameters[stagingDirectory]}")
    private String stagingDirectory;
    
    @Resource(name="caseListMetadataMap")
    public Map<String, String> caseListMetadataMap;
    
    private CancerStudy cancerStudy;
    private List<SampleList> sampleListResults;
    
    private static final Log LOG = LogFactory.getLog(CaseListReader.class);

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cancerStudy = (CancerStudy) executionContext.get("cancerStudy");
        MultiKeyMap datatypeMetadata = (MultiKeyMap) executionContext.get("datatypeMetadata");
        File caseListDirectory = new File(stagingDirectory, "case_lists");
        
        // load standard sample lists (i.e., cases_all, cases_cna, cases_sequenced, cases_cnaseq)
        List<SampleList> sampleLists = new ArrayList();
        try {
            LOG.info("Loading standard case lists from: " + stagingDirectory + "/case_lists");
            sampleLists = loadCaseLists(caseListDirectory.getCanonicalPath(), datatypeMetadata);
        }
        catch (IOException ex) {
            LOG.error("Error loading case lists from case list directory: " + stagingDirectory + "/case_lists");
            ex.printStackTrace();
        }
        
        // load custom case lists if any
        List<File> customCaseListFiles = new ArrayList();
        try {
            List<File> caseListFiles = DataFileUtils.listDataFiles(caseListDirectory.getCanonicalPath(), "case*");
            for (File caseListFile : caseListFiles) {
                if (!caseListMetadataMap.containsKey(caseListFile.getName())) {
                    customCaseListFiles.add(caseListFile);
                }
            }
        } 
        catch (IOException ex) {}
        
        if (!customCaseListFiles.isEmpty()) {
            LOG.info("Loading custom case lists found");
            for (File caseListFile : customCaseListFiles) {
                Properties properties = new Properties();
                try {
                    properties.load(new FileInputStream(caseListFile));
                    Set<Integer> caseIdSet = loadCaseIdSetFromCaseListFile(properties);
                    if (!caseIdSet.isEmpty()) {                        
                        SampleList sampleList = loadSampleListFromFile(properties, new ArrayList(caseIdSet));
                        sampleLists.add(sampleList);
                        LOG.info("Loaded " + caseIdSet.size() + " cases from case list file: " + caseListFile.getName());
                    }
                    else {
                        LOG.error("Error loading custom case list from: " + caseListFile.getName());
                    }
                } 
                catch (IOException ex) {}
            }
        }
        
        this.sampleListResults = sampleLists;
    }

    /**
     * Load case lists from the case list directory.
     * 
     * @param caseListDirectory
     * @param datatypeMetadata
     * @return List<SampleList>
     * @throws IOException 
     */
    private List<SampleList> loadCaseLists(String caseListDirectory, MultiKeyMap datatypeMetadata) throws IOException {
        List<SampleList> sampleLists = new ArrayList();
        for (String caseListFilename : caseListMetadataMap.keySet()) {
            // if case list filename does not exist then continue
            File caseListFile = new File(caseListDirectory, caseListFilename);
            if (!caseListFile.exists()) {
                continue;
            }
            
            boolean autoGenerateCaseList = true;
            // load properties from case list file
            Properties properties = new Properties();
            properties.load(new FileInputStream(caseListFile));
            if (properties.containsKey("case_list_ids")) {
                autoGenerateCaseList = false;
            }

            // get the case id set either from the case list file or from what was imported
            // for the case list datatypes
            Set<Integer> caseIdSet = new LinkedHashSet<>();            
            if (!autoGenerateCaseList) {
                caseIdSet = loadCaseIdSetFromCaseListFile(properties);
            }
            else {
                // overwrite case list only applies to case list filename "cases_complete.txt"
                // if RNA-SEQ data exists, we want the cases complete file to contain it, not micro-array gene expression (Agilent).
                boolean overwriteCaseList = caseListFilename.contains("complete");
                String[] datatypeList = caseListMetadataMap.get(caseListFilename).split("\\|");

                Set<Integer> workingCaseIdSet = new LinkedHashSet<>();
                for (String datatypeGroup : datatypeList) {
                    String[] datatypes = datatypeGroup.split("\\&");
                    
                    if (!(boolean) datatypeMetadata.get(datatypes[0], "importData")) {
                        continue;
                    }
                    
                    // only overwrite working case id set for cases_complete.txt                    
                    if (overwriteCaseList) {
                        workingCaseIdSet = (Set<Integer>) datatypeMetadata.get(datatypes[0], "caseList");
                    }
                    else {
                        workingCaseIdSet.addAll((Set<Integer>) datatypeMetadata.get(datatypes[0], "caseList"));
                    }

                    // get intersection of datatypes if necessary
                    if (datatypes.length > 1 ) {
                        for (int i=1; i<=datatypes.length; i++) {
                            if (!(boolean) datatypeMetadata.get(datatypes[i], "importData")) {
                                continue;
                            }
                            workingCaseIdSet = Sets.intersection(workingCaseIdSet, (Set<Integer>) datatypeMetadata.get(datatypes[i], "caseList"));
                        }
                    }
                }
                caseIdSet.addAll(workingCaseIdSet);
            }
            // get the remaining case list file properties if the case id set is not empty
            if (caseIdSet.isEmpty()) {
                continue;
            }                        
            SampleList sampleList = loadSampleListFromFile(properties, new ArrayList(caseIdSet));            
            sampleLists.add(sampleList);
            LOG.info("Retrieved case list: " + sampleList.getStableId());
        }
        
        return sampleLists;
    }
    
    /**
     * Load case id set directly from a case list file properties.
     * 
     * @param properties
     * @return Set<Integer>
     * @throws IOException 
     */
    private Set<Integer> loadCaseIdSetFromCaseListFile(Properties properties) throws IOException {
        Set<Integer> caseIdSet = new LinkedHashSet<>();
        
        // get sample internal id for each case id in list
        String[] caseIds = DataFileUtils.splitDataFields(properties.getProperty("case_list_ids"));
        for (String caseId : caseIds) {
            String sampleStableId = DataFileUtils.getSampleStableId(caseId);
            Sample sample = sampleJdbcDaoImpl.getSampleByStudy(sampleStableId, cancerStudy.getCancerStudyId());
            if (sample == null) {
                LOG.warn("Skipping unknown sample stable id: " + (sampleStableId.equals(caseId)?caseId:sampleStableId));
                continue;
            }
            caseIdSet.add(sample.getInternalId());
        }
        
        return caseIdSet;
    }
    
    /**
     * Load sample list from file given case list file properties and the set of case ids.
     * 
     * @param properties
     * @param caseIdSet
     * @return SampleList
     */
    private SampleList loadSampleListFromFile(Properties properties, List<Integer> caseIdSet) {
        String stableId = properties.getProperty("stable_id");
        String category = properties.getProperty("case_list_category", "other");
        String name = properties.getProperty("case_list_name");
        String description = properties.getProperty("case_list_description");

        SampleList sampleList = new SampleList();
        sampleList.setStableId(stableId);
        sampleList.setCategory(category);
        sampleList.setName(name);
        sampleList.setDescription(description);
        sampleList.setCancerStudyId(cancerStudy.getCancerStudyId());
        sampleList.setSampleListList(caseIdSet);

        return sampleList;
    }
        
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public SampleList read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!sampleListResults.isEmpty()) {
            return sampleListResults.remove(0);
        }
        return null;
    }
    
}

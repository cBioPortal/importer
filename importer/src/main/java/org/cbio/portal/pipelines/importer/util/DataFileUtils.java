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

package org.cbio.portal.pipelines.importer.util;

import org.mskcc.cbio.model.Sample;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import com.google.common.base.Strings;
import org.apache.commons.collections.map.MultiKeyMap;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * General utils class for files and file data.
 * 
 * @author ochoaa
 */
@Repository
public class DataFileUtils {
    
    // general constants
    public static String DELIMITER = "\t";
    public static String METADATA_PREFIX = "#";
    public static String DEFAULT_DESCRIPTION = "MISSING";    
    public static String DEFAULT_DATATYPE = "STRING";
    public static String DEFAULT_ATTRIBUTE_TYPE = "SAMPLE";
    public static String DEFAULT_PRIORITY = "1";
    public static String DEFAULT_NOT_AVAILABLE = "NA";
    public static Integer DEFAULT_MISSING_INT = -1;
    public static Integer DATA_TRUNCATION_THRESHOLD = 255;
    
    // regex expressions for stable id's
    public static final String TCGA_BARCODE_PREFIX = "TCGA";
    public static final Pattern TCGA_SAMPLE_BARCODE_REGEX =
        Pattern.compile("^(TCGA-\\w\\w-\\w\\w\\w\\w-\\d\\d).*$");
    public static final Pattern TCGA_SAMPLE_TYPE_BARCODE_REGEX =
        Pattern.compile("^TCGA-\\w\\w-\\w\\w\\w\\w-(\\d\\d).*$");
    
    public static final Set<String> nonCaseIdColumnNames = new HashSet(Arrays.asList(
            new String[]{"GENE SYMBOL", "HUGO_SYMBOL", "ENTREZ_GENE_ID", 
                "LOCUS ID", "CYTOBAND", "COMPOSITE.ELEMENT.REF"}));
    
    public static final HashMap<String, Sample.SampleType> sampleTypeByTcgaCode = new HashMap<>();
    @Autowired
    private void setSampleTypeByTcgaCode() {
        sampleTypeByTcgaCode.put("01", Sample.SampleType.PRIMARY_SOLID_TUMOR);
        sampleTypeByTcgaCode.put("02", Sample.SampleType.RECURRENT_SOLID_TUMOR);
        sampleTypeByTcgaCode.put("03", Sample.SampleType.PRIMARY_BLOOD_TUMOR);
        sampleTypeByTcgaCode.put("04", Sample.SampleType.RECURRENT_BLOOD_TUMOR);
        sampleTypeByTcgaCode.put("06", Sample.SampleType.METASTATIC);
        sampleTypeByTcgaCode.put("10", Sample.SampleType.BLOOD_NORMAL);
        sampleTypeByTcgaCode.put("11", Sample.SampleType.SOLID_NORMAL);
    }

    public static enum NullOrEmptyValues {
        NOT_APPLICABLE("Not Applicable"),
        NOT_AVAILABLE("Not Available"),
        SENT("sent"),
        NULL("null"),
        MISSING(""),
        NA("NA"),
        N_A("N/A");

        private final String propertyName;
        
        NullOrEmptyValues(String propertyName) { this.propertyName = propertyName; }
        @Override
        public String toString() { return propertyName; }

        static public boolean has(String value) {
            if (value == null) return false;
            if (value.trim().equals("")) return true;
            try { 
                value = value.replaceAll("[\\[|\\]\\/]", "");
                value = value.replaceAll(" ", "_");
                return valueOf(value.toUpperCase()) != null;
            }
            catch (IllegalArgumentException ex) {
                return false;
            }
        }
    }
    
    /**
     * List data files in directory by file pattern.
     * 
     * @param directory
     * @param filePattern
     * @return List<File>
     */
    public static List<File> listDataFiles(String directory, String filePattern) throws IOException {
     List<File> dataFiles = new ArrayList();
     
     for (Path file : Files.newDirectoryStream(Paths.get(directory), filePattern)) {
         dataFiles.add(file.toFile());
     }
     
     return dataFiles;
    }
    
    /**
     * List data files in directory by file pattern with filename filter.
     * 
     * @param directory
     * @param filePattern
     * @param filter
     * @return List<File>
     */
    public static List<File> listDataFiles(String directory, String filePattern, String filter) throws IOException {
        if (Strings.isNullOrEmpty(filter)) {
            return listDataFiles(directory, filePattern);
        }
        
        List<File> dataFiles = new ArrayList();
        for (Path file : Files.newDirectoryStream(Paths.get(directory), filePattern)) {
            if (!file.toFile().getName().contains(filter)) {
                dataFiles.add(file.toFile());
            }
        }
     
        return dataFiles;
    }
    
    /**
     * Light string processing/cleanup for tab delimited files.
     * 
     * @param line
     * @return String[]
     */
    public static String[] splitDataFields(String line) {
        line = line.replaceAll("^" + METADATA_PREFIX + "+", "");
        String[] fields = line.split(DELIMITER, -1);

        return fields;                   
    }
    
    /**
     * Returns the header of a given datafile.
     * 
     * @param dataFile
     * @return String[]
     * @throws IOException 
     */
    public static String[] getFileHeader(File dataFile) throws IOException {
        String[] columnNames;
        
        try (FileReader reader = new FileReader(dataFile)) {
            BufferedReader buff = new BufferedReader(reader);            
            String line = buff.readLine();
            
            // keep reading until line does not start with meta data prefix
            while (line.startsWith(METADATA_PREFIX)) {
                line = buff.readLine();
            }
            // extract the maf file header
            columnNames = splitDataFields(line);
            reader.close();
        }
        
        return columnNames;
    }
    
    /**
     * Loads the datafile metadata (file header and number of records).
     * 
     * @param dataFile
     * @return MultiKeyMap
     * @throws IOException 
     */
    public static MultiKeyMap loadDataFileMetadata(File dataFile) throws IOException {
        String[] columnNames;
        int numRecords = 0;
        
        // get the file header and record count
        try (FileReader reader = new FileReader(dataFile)) {
            BufferedReader buff = new BufferedReader(reader);            
            String line = buff.readLine();
            
            // keep reading until line does not start with meta data prefix
            while (line.startsWith(DataFileUtils.METADATA_PREFIX)) {
                line = buff.readLine();
            }
            // extract the file header
            columnNames = DataFileUtils.splitDataFields(line);
            
            // keep reading file to get count of records
            while (buff.readLine() != null) {
                numRecords++;
            }
            reader.close();
        }
        MultiKeyMap metadata = new MultiKeyMap();
        metadata.put(dataFile.getName(), "header", columnNames);
        metadata.put(dataFile.getName(), "numRecords", numRecords);
        
        return metadata;
    }
    
    /**
     * Determines if string is null, empty, or 'null' value.
     * 
     * @param value
     * @return boolean
     */
    public static boolean isNullOrEmptyValue(String value) {
        return Strings.isNullOrEmpty(value) || NullOrEmptyValues.has(value);
    }
    
    /**
     * Determines if integer string is null/empty or default missing int.
     * 
     * @param value
     * @return boolean
     */
    public static boolean isNullOrMissingInt(String value) {
        return isNullOrEmptyValue(value) || Integer.valueOf(value).equals(DEFAULT_MISSING_INT);
    }
    
    /**
     * Formats a given string for enum SampleType.
     * 
     * @param barcode
     * @param sampleTypeValue
     * @return String
     */
    public static String getSampleTypeString(String barcode, String sampleTypeValue) {
        // set default to sample type value given from sample
        String sampleTypeString = Sample.SampleType.PRIMARY_SOLID_TUMOR.getName();
        
        // if tcga barcode then try to get sample type by tcga code
        if (barcode.startsWith(TCGA_BARCODE_PREFIX)) {
            Matcher tcgaSampleBarcodeMatcher = TCGA_SAMPLE_TYPE_BARCODE_REGEX.matcher(barcode);
            if (tcgaSampleBarcodeMatcher.find() && sampleTypeByTcgaCode.containsKey(tcgaSampleBarcodeMatcher.group(1))) {
                sampleTypeString = sampleTypeByTcgaCode.get(tcgaSampleBarcodeMatcher.group(1)).getName();
            }
        }
        else if (!Strings.isNullOrEmpty(sampleTypeValue)) {
            sampleTypeString = sampleTypeValue;
        }
        
        return StringUtils.join(sampleTypeString.trim().split(" "), "_").toUpperCase();
    }
    
    /**
     * Returns a patient stable id.
     * 
     * @param stableId
     * @return String
     */
    public static String getPatientStableId(String stableId) {
        return getStableId(stableId, false);
    }
    
    /**
     * Returns a sample stable ID.
     * 
     * @param stableId
     * @return String
     */
    public static String getSampleStableId(String stableId) {
        return getStableId(stableId, true);
    }
    
    /**
     * Cleans up TCGA stable ID's.
     * 
     * @param barcode
     * @param forSample
     * @return String
     */
    private static String getStableId(String barcode, boolean forSample) {
        if (!barcode.startsWith(TCGA_BARCODE_PREFIX)) {
            return barcode;
        }
        // light clean up on the tcga barcode
        if (barcode.contains("Tumor")) {
            barcode = barcode.replace("Tumor", "01");
        }
        else if (barcode.contains("Normal")) {
            barcode = barcode.replace("Normal", "11");
        }
        
        // set default stable id string        
        String stableId = barcode + "-01";
        try {
            String[] parts = barcode.split("-");
            List<String> stableIdBuilder = new ArrayList(
                    Arrays.asList(new String[]{parts[0], parts[1], parts[2]}));
            
            if (forSample) {
                stableIdBuilder.add(parts[3]);
                Matcher tcgaSampleBarcodeMatcher = TCGA_SAMPLE_BARCODE_REGEX.matcher(StringUtils.join(stableIdBuilder, "-"));
                stableId = (tcgaSampleBarcodeMatcher.find()) ? tcgaSampleBarcodeMatcher.group(1) : StringUtils.join(stableIdBuilder, "-");
            }
            else {
                stableId = StringUtils.join(stableIdBuilder, "-");
            }
        }
        catch (ArrayIndexOutOfBoundsException ex) {}
        
        
        return stableId;
    }

    /**
     * Determines whether TCGA sample type is normal or not.
     * 
     * @param barcode
     * @return boolean
     */
    public static boolean isNormalSample(String barcode) {
        Matcher tcgaSampleBarcodeMatcher = TCGA_SAMPLE_TYPE_BARCODE_REGEX.matcher(barcode);
        
        Sample.SampleType sampleType = Sample.SampleType.PRIMARY_SOLID_TUMOR;
        if (tcgaSampleBarcodeMatcher.find() && sampleTypeByTcgaCode.containsKey(tcgaSampleBarcodeMatcher.group(1))) {
                sampleType = sampleTypeByTcgaCode.get(tcgaSampleBarcodeMatcher.group(1));
        }
        
        boolean isNormal = false;
        if (sampleType.getName().equals(Sample.SampleType.BLOOD_NORMAL.getName()) || 
                sampleType.getName().equals(Sample.SampleType.SOLID_NORMAL.getName())) {
            isNormal = true;
        }
        
        return isNormal;
    }

}

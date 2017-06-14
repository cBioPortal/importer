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

import org.mskcc.cbio.model.Gene;
import org.mskcc.cbio.persistence.jdbc.GeneJdbcDaoImpl;

import java.util.*;
import java.util.regex.*;
import org.apache.commons.logging.*;
import com.google.common.base.Strings;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.*;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Repository;

/**
 * Utils class for gene data 
 * @author ochoaa
 */
@Repository
@JobScope
public class GeneDataUtils {
    
    public final String PHOSPHOPROTEIN_TYPE = "phosphoprotein";
    public final String MIRNA_TYPE = "miRNA";
    
    private final String DISAMBIGUOUS_GENES_RESOURCE = "gene_symbol_disambiguation.txt";
    
    @Autowired
    GeneJdbcDaoImpl geneJdbcDaoImpl;
    
    private static final Log LOG = LogFactory.getLog(GeneDataUtils.class);
    
    // params to cache genes in db
    public static final Map<String, Gene> hugoGeneSymbolMap = new HashMap<>();
    public static final Map<Integer, Gene> entrezGeneIdMap = new HashMap<>();
    public static final Map<String, List<Gene>> geneAliasMap = new HashMap<>();
    @Autowired
    private void loadGenesFromDb() {
        for (Gene gene : geneJdbcDaoImpl.listAllGenes()) {
            // fill gene symbol, gene id, and gene alias maps
            hugoGeneSymbolMap.put(gene.getHugoGeneSymbol(), gene);
            entrezGeneIdMap.put(gene.getEntrezGeneId(), gene);
            
            if (gene.getAliases() != null) {
                gene.getAliases().stream().forEach((alias) -> {
                    geneAliasMap.getOrDefault(alias, new ArrayList()).add(gene);
                });    
            }
        }
    }
    
    // param to hold disambiguous genes
    public static final Map<String, Gene> disambiguousGenes = new HashMap<>();
    @Autowired
    private void loadDisambiguousGenes() throws Exception {
        // init tab-delim tokenizer with column names
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB);
        tokenizer.setNames(new String[]{"Gene_Symbol", "Entrez_Gene_Id"});
        
        // init line mapper for disambiguous gene file resource
        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper((FieldSet fs) -> {
            HashMap<String, Object> map = new HashMap<>();
            map.put("alias", fs.readString("Gene_Symbol"));
            map.put("entrezGeneId", fs.readInt("Entrez_Gene_Id"));
            return map;
        });
        
        // set up file reader context
        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader();
        reader.setResource(new ClassPathResource(DISAMBIGUOUS_GENES_RESOURCE));
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        reader.open(new ExecutionContext());
        
        // read through each line of disambiguous genes file
        Map<String, Object> record = reader.read();
        while (record != null) {
            String alias = (String) record.get("alias");
            Gene gene = geneJdbcDaoImpl.getGene((Integer) record.get("entrezGeneId"));
            if (gene == null) {
                LOG.warn("Could not resolve disambiguous gene alias " + alias + " from " + DISAMBIGUOUS_GENES_RESOURCE);
            }
            else {
                disambiguousGenes.put(alias, gene);
            }
            record = reader.read();
        }
        reader.close();
    }
    
    // param to hold valid chromosomes
    public static final Map<String, String> validChromosomeValues = new HashMap<>();
    @Autowired
    private void setValidChromosomeValues() {
        // init valid chromosome values map
        for (int chr=1; chr<=24; chr++) {
            validChromosomeValues.put(String.valueOf(chr), String.valueOf(chr));
            validChromosomeValues.put("CHR"+String.valueOf(chr), String.valueOf(chr));
        }
        // add other valid chromosome values to map        
        validChromosomeValues.put("X", "23");
        validChromosomeValues.put("CHRX", "23");
        validChromosomeValues.put("Y", "24");
        validChromosomeValues.put("CHRY", "24");
        validChromosomeValues.put("NA", "NA");
        validChromosomeValues.put("MT", "MT");
    }
    
    /**
     * Given an entrez gene id, hugo gene symbol, and chromosome, returns a Gene instance.
     * If gene is not found, returns null
     * 
     * @param entrezGeneId
     * @param hugoGeneSymbol
     * @param chr
     * @return Gene
     */
    public Gene resolveGeneFromRecordData(Integer entrezGeneId, String hugoGeneSymbol, String chr) {
        Gene gene;
        
        // first try to get gene by entrez gene id
        if (entrezGeneIdMap.containsKey(entrezGeneId)) {
            gene = entrezGeneIdMap.get(entrezGeneId);
        }
        else if (hugoGeneSymbolMap.containsKey(hugoGeneSymbol)) {
            gene = hugoGeneSymbolMap.get(hugoGeneSymbol);
        }
        // if gene does not exist by given entrez gene id or hugo gene symbol then guess gene
        else {            
            gene = guessGene(hugoGeneSymbol, chr);
        }
        
        return gene;
    }
    
    /**
     * Given an hugo gene symbol and chromosome, returns a Gene instance.
     * If gene is not found, returns null
     * 
     * @param hugoGeneSymbol
     * @param chr
     * @return Gene
     */
    public Gene resolveGeneFromRecordData(String hugoGeneSymbol, String chr) {
        Gene gene;
        
        // first try to get gene by hugo gene symbol
        if (hugoGeneSymbolMap.containsKey(hugoGeneSymbol)) {
            gene = hugoGeneSymbolMap.get(hugoGeneSymbol);
        }
        // if gene does not exist by given entrez gene id or hugo gene symbol then guess gene
        else {            
            gene = guessGene(hugoGeneSymbol, chr);
        }
        
        return gene;
    }
    
    /**
     * Given an array of gene symbols, returns a list of Gene instances.
     * If all gene symbols cannot be found, returns empty list
     * 
     * @param geneSymbols
     * @return List<Gene>
     */
    public List<Gene> resolveGeneFromCompositeElementRef(String[] geneSymbols) {
        List<Gene> genes = new ArrayList();
        for (String geneSymbol : geneSymbols) {
            Gene gene = guessGene(geneSymbol, null);
            if (gene != null) {
                genes.add(gene);
            }
            else {
                LOG.warn("Could not resolve gene from symbol: " + geneSymbol);
            }
        }
        
        return genes;
    }
    
    /**
     * Guesses gene by given hugo gene symbol and/or chromosome value.
     * 
     * @param hugoGeneSymbol
     * @param chr
     * @return Gene
     */
    public Gene guessGene(String hugoGeneSymbol, String chr) {
        Gene gene = null; // default value
        
        if (!Strings.isNullOrEmpty(hugoGeneSymbol)) {
            //  hugo gene symbol possibly entrez gene id instead
            if (hugoGeneSymbol.matches("[0-9]+") && entrezGeneIdMap.containsKey(Integer.valueOf(hugoGeneSymbol))) {                
                gene = entrezGeneIdMap.get(Integer.valueOf(hugoGeneSymbol));            
            }
            // try finding hugo gene symbol in map
            else if (hugoGeneSymbolMap.containsKey(hugoGeneSymbol.toUpperCase())) {
                gene = hugoGeneSymbolMap.get(hugoGeneSymbol.toUpperCase());
            }
            // try finding gene by disambiguous gene symbol
            else if (disambiguousGenes.containsKey(hugoGeneSymbol.toUpperCase())) {
                gene = disambiguousGenes.get(hugoGeneSymbol.toUpperCase());
            }
            // try finding gene by alias and normalized chromosome value
            else if (geneAliasMap.containsKey(hugoGeneSymbol)) {
                
                // get normalized chromosome value
                String normalizedChr = getNormalizedChromosome(chr);
                if (!Strings.isNullOrEmpty(normalizedChr)) {
                    
                    // add gene aliases with matching chromosome values to list
                    List<Gene> matchesByChrValue = new ArrayList();
                    for (Gene alias : geneAliasMap.get(hugoGeneSymbol)) {                        
                        // get normalized chromosome value for alias
                        String aliasChrValue = getChromosomeFromCytoband(alias.getCytoband());
                        if (aliasChrValue.equals(normalizedChr)) {
                            matchesByChrValue.add(alias);
                        }
                    }                    
                    // if list not empty then select first in list by default
                    if (!matchesByChrValue.isEmpty()) {
                        gene = matchesByChrValue.get(0);
                    }
                }
            }
        }

        return gene;
    }
    
    /**
     * Returns a phospho gene with aliases. 
     * 
     * @param gene
     * @param residue
     * @return Gene
     */
    public Gene createPhosphoGene(Gene gene, String residue) {
        Gene phosphoGene = new Gene();
        phosphoGene.setHugoGeneSymbol(gene.getHugoGeneSymbol().toUpperCase() + "_" + residue);
        phosphoGene.setType(PHOSPHOPROTEIN_TYPE);
        phosphoGene.setCytoband(gene.getCytoband());
        phosphoGene.setAliases(Arrays.asList(new String[]{"rppa-phospho", 
                "phosphoprotein", "phospho"+gene.getHugoGeneSymbol().toUpperCase()}));
        
        return phosphoGene;
    }
    
    /**
     * Return normalized chromosome value.
     * If not found, returns null
     * 
     * @param chr
     * @return String
     */
    public String getNormalizedChromosome(String chr) {
        if (!Strings.isNullOrEmpty(chr) && !DataFileUtils.isNullOrEmptyValue(chr)) {
            return validChromosomeValues.get(chr.toUpperCase());
        }
        return null;
    }
    
    /**
     * Returns normalized chromosome value from cytoband.
     * If not found, returns null
     * 
     * @param cytoband
     * @return String
     */
    public String getChromosomeFromCytoband(String cytoband) {
        String chromosome = null;
        if (!Strings.isNullOrEmpty(cytoband)) {
            if (cytoband.startsWith("X") || cytoband.startsWith("Y")) {
                chromosome = cytoband.substring(0, 1);
            }
            else {
                Pattern p = Pattern.compile("([0-9]+).*");
                Matcher m = p.matcher(cytoband);
                if (m.find()) {
                    chromosome = m.group(1);
                }
            }
        }
        return getNormalizedChromosome(chromosome);
    }
    
}

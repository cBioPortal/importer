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
public class MafRecord {
    
    private Integer sampleId;
    private Integer geneticProfileId;
    private String hugoSymbol;
    private String entrezGeneId;
    private String center;
    private String ncbiBuild;
    private String chromosome;
    private String startPosition;
    private String endPosition;
    private String strand;
    private String variantClassification;
    private String variantType;
    private String referenceAllele;
    private String tumorSeqAllele1;
    private String tumorSeqAllele2;
    private String dbsnpRs;
    private String dbsnpValStatus;
    private String tumorSampleBarcode;
    private String matchedNormSampleBarcode;
    private String matchNormSeqAllele1;
    private String matchNormSeqAllele2;
    private String tumorValidationAllele1;
    private String tumorValidationAllele2;
    private String matchNormValidationAllele1;
    private String matchNormValidationAllele2;
    private String verificationStatus;
    private String validationStatus;
    private String mutationStatus;
    private String sequencingPhase;
    private String sequenceSource;
    private String validationMethod;
    private String score;
    private String bamFile;
    private String sequencer;
    private String aminoAcidChange;
    private String transcript;
    private String tRefCount;
    private String tAltCount;
    private String nRefCount;
    private String nAltCount;
    private String tTotCov;
    private String tVarCov;
    private String nTotCov;
    private String nVarCov;
    private String tumorDepth;
    private String tumorVaf;
    private String normalDepth;
    private String normalVaf;
    private String hgvspShort;
    private String codons;
    private String swissprot;
    private String refseq;
    private String proteinPosition;
    private String oncotatorCosmicOverlapping;
    private String oncotatorDbsnpRs;
    private String oncotatorDbsnpValStatus;
    private String oncotatorProteinChange;
    private String oncotatorVariantClassification;
    private String oncotatorGeneSymbol;
    private String oncotatorRefseqMrnaId;
    private String oncotatorRefseqProtId;
    private String oncotatorUniprotEntryName;
    private String oncotatorUniprotAccession;
    private String oncotatorCodonChange;
    private String oncotatorTranscriptChange;
    private String oncotatorExonAffected;
    private String oncotatorProteinPosStart;
    private String oncotatorProteinPosEnd;
    private String oncotatorProteinChangeBe;
    private String oncotatorVariantClassificationBe;
    private String oncotatorGeneSymbolBe;
    private String oncotatorRefseqMrnaIdBe;
    private String oncotatorRefseqProtIdBe;
    private String oncotatorUniprotEntryNameBe;
    private String oncotatorUniprotAccessionBe;
    private String oncotatorCodonChangeBe;
    private String oncotatorTranscriptChangeBe;
    private String oncotatorExonAffectedBe;
    private String oncotatorProteinPosStartBe;
    private String oncotatorProteinPosEndBe;
    private String maFimpact;
    private String maFis;
    private String maLinkVar;
    private String maLinkMsa;
    private String maLinkPdb;
    private String maProteinChange;

    /**
     * @return the sampleId
     */
    public Integer getSampleId() {
        return sampleId;
    }

    /**
     * @param sampleId the sampleId to set
     */
    public void setSampleId(Integer sampleId) {
        this.sampleId = sampleId;
    }

    /**
     * @return the geneticProfileId
     */
    public Integer getGeneticProfileId() {
        return geneticProfileId;
    }

    /**
     * @param geneticProfileId the geneticProfileId to set
     */
    public void setGeneticProfileId(Integer geneticProfileId) {
        this.geneticProfileId = geneticProfileId;
    }

    /**
     * @return the hugoSymbol
     */
    public String getHugoSymbol() {
        return hugoSymbol;
    }

    /**
     * @param hugoSymbol the hugoSymbol to set
     */
    public void setHugoSymbol(String hugoSymbol) {
        this.hugoSymbol = hugoSymbol;
    }

    /**
     * @return the entrezGeneId
     */
    public String getEntrezGeneId() {
        return entrezGeneId;
    }

    /**
     * @param entrezGeneId the entrezGeneId to set
     */
    public void setEntrezGeneId(String entrezGeneId) {
        this.entrezGeneId = entrezGeneId;
    }

    /**
     * @return the center
     */
    public String getCenter() {
        return center;
    }

    /**
     * @param center the center to set
     */
    public void setCenter(String center) {
        this.center = center;
    }

    /**
     * @return the ncbiBuild
     */
    public String getNcbiBuild() {
        return ncbiBuild;
    }

    /**
     * @param ncbiBuild the ncbiBuild to set
     */
    public void setNcbiBuild(String ncbiBuild) {
        this.ncbiBuild = ncbiBuild;
    }

    /**
     * @return the chromosome
     */
    public String getChromosome() {
        return chromosome;
    }

    /**
     * @param chromosome the chromosome to set
     */
    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    /**
     * @return the startPosition
     */
    public String getStartPosition() {
        return startPosition;
    }

    /**
     * @param startPosition the startPosition to set
     */
    public void setStartPosition(String startPosition) {
        this.startPosition = startPosition;
    }

    /**
     * @return the endPosition
     */
    public String getEndPosition() {
        return endPosition;
    }

    /**
     * @param endPosition the endPosition to set
     */
    public void setEndPosition(String endPosition) {
        this.endPosition = endPosition;
    }

    /**
     * @return the strand
     */
    public String getStrand() {
        return strand;
    }

    /**
     * @param strand the strand to set
     */
    public void setStrand(String strand) {
        this.strand = strand;
    }

    /**
     * @return the variantClassification
     */
    public String getVariantClassification() {
        return variantClassification;
    }

    /**
     * @param variantClassification the variantClassification to set
     */
    public void setVariantClassification(String variantClassification) {
        this.variantClassification = variantClassification;
    }

    /**
     * @return the variantType
     */
    public String getVariantType() {
        return variantType;
    }

    /**
     * @param variantType the variantType to set
     */
    public void setVariantType(String variantType) {
        this.variantType = variantType;
    }

    /**
     * @return the referenceAllele
     */
    public String getReferenceAllele() {
        return referenceAllele;
    }

    /**
     * @param referenceAllele the referenceAllele to set
     */
    public void setReferenceAllele(String referenceAllele) {
        this.referenceAllele = referenceAllele;
    }

    /**
     * @return the tumorSeqAllele1
     */
    public String getTumorSeqAllele1() {
        return tumorSeqAllele1;
    }

    /**
     * @param tumorSeqAllele1 the tumorSeqAllele1 to set
     */
    public void setTumorSeqAllele1(String tumorSeqAllele1) {
        this.tumorSeqAllele1 = tumorSeqAllele1;
    }

    /**
     * @return the tumorSeqAllele2
     */
    public String getTumorSeqAllele2() {
        return tumorSeqAllele2;
    }

    /**
     * @param tumorSeqAllele2 the tumorSeqAllele2 to set
     */
    public void setTumorSeqAllele2(String tumorSeqAllele2) {
        this.tumorSeqAllele2 = tumorSeqAllele2;
    }

    /**
     * @return the dbsnpRs
     */
    public String getDbsnpRs() {
        return dbsnpRs;
    }

    /**
     * @param dbsnpRs the dbsnpRs to set
     */
    public void setDbsnpRs(String dbsnpRs) {
        this.dbsnpRs = dbsnpRs;
    }

    /**
     * @return the dbsnpValStatus
     */
    public String getDbsnpValStatus() {
        return dbsnpValStatus;
    }

    /**
     * @param dbsnpValStatus the dbsnpValStatus to set
     */
    public void setDbsnpValStatus(String dbsnpValStatus) {
        this.dbsnpValStatus = dbsnpValStatus;
    }

    /**
     * @return the tumorSampleBarcode
     */
    public String getTumorSampleBarcode() {
        return tumorSampleBarcode;
    }

    /**
     * @param tumorSampleBarcode the tumorSampleBarcode to set
     */
    public void setTumorSampleBarcode(String tumorSampleBarcode) {
        this.tumorSampleBarcode = tumorSampleBarcode;
    }

    /**
     * @return the matchedNormSampleBarcode
     */
    public String getMatchedNormSampleBarcode() {
        return matchedNormSampleBarcode;
    }

    /**
     * @param matchedNormSampleBarcode the matchedNormSampleBarcode to set
     */
    public void setMatchedNormSampleBarcode(String matchedNormSampleBarcode) {
        this.matchedNormSampleBarcode = matchedNormSampleBarcode;
    }

    /**
     * @return the matchNormSeqAllele1
     */
    public String getMatchNormSeqAllele1() {
        return matchNormSeqAllele1;
    }

    /**
     * @param matchNormSeqAllele1 the matchNormSeqAllele1 to set
     */
    public void setMatchNormSeqAllele1(String matchNormSeqAllele1) {
        this.matchNormSeqAllele1 = matchNormSeqAllele1;
    }

    /**
     * @return the matchNormSeqAllele2
     */
    public String getMatchNormSeqAllele2() {
        return matchNormSeqAllele2;
    }

    /**
     * @param matchNormSeqAllele2 the matchNormSeqAllele2 to set
     */
    public void setMatchNormSeqAllele2(String matchNormSeqAllele2) {
        this.matchNormSeqAllele2 = matchNormSeqAllele2;
    }

    /**
     * @return the tumorValidationAllele1
     */
    public String getTumorValidationAllele1() {
        return tumorValidationAllele1;
    }

    /**
     * @param tumorValidationAllele1 the tumorValidationAllele1 to set
     */
    public void setTumorValidationAllele1(String tumorValidationAllele1) {
        this.tumorValidationAllele1 = tumorValidationAllele1;
    }

    /**
     * @return the tumorValidationAllele2
     */
    public String getTumorValidationAllele2() {
        return tumorValidationAllele2;
    }

    /**
     * @param tumorValidationAllele2 the tumorValidationAllele2 to set
     */
    public void setTumorValidationAllele2(String tumorValidationAllele2) {
        this.tumorValidationAllele2 = tumorValidationAllele2;
    }

    /**
     * @return the matchNormValidationAllele1
     */
    public String getMatchNormValidationAllele1() {
        return matchNormValidationAllele1;
    }

    /**
     * @param matchNormValidationAllele1 the matchNormValidationAllele1 to set
     */
    public void setMatchNormValidationAllele1(String matchNormValidationAllele1) {
        this.matchNormValidationAllele1 = matchNormValidationAllele1;
    }

    /**
     * @return the matchNormValidationAllele2
     */
    public String getMatchNormValidationAllele2() {
        return matchNormValidationAllele2;
    }

    /**
     * @param matchNormValidationAllele2 the matchNormValidationAllele2 to set
     */
    public void setMatchNormValidationAllele2(String matchNormValidationAllele2) {
        this.matchNormValidationAllele2 = matchNormValidationAllele2;
    }

    /**
     * @return the verificationStatus
     */
    public String getVerificationStatus() {
        return verificationStatus;
    }

    /**
     * @param verificationStatus the verificationStatus to set
     */
    public void setVerificationStatus(String verificationStatus) {
        this.verificationStatus = verificationStatus;
    }

    /**
     * @return the validationStatus
     */
    public String getValidationStatus() {
        return validationStatus;
    }

    /**
     * @param validationStatus the validationStatus to set
     */
    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    /**
     * @return the mutationStatus
     */
    public String getMutationStatus() {
        return mutationStatus;
    }

    /**
     * @param mutationStatus the mutationStatus to set
     */
    public void setMutationStatus(String mutationStatus) {
        this.mutationStatus = mutationStatus;
    }

    /**
     * @return the sequencingPhase
     */
    public String getSequencingPhase() {
        return sequencingPhase;
    }

    /**
     * @param sequencingPhase the sequencingPhase to set
     */
    public void setSequencingPhase(String sequencingPhase) {
        this.sequencingPhase = sequencingPhase;
    }

    /**
     * @return the sequenceSource
     */
    public String getSequenceSource() {
        return sequenceSource;
    }

    /**
     * @param sequenceSource the sequenceSource to set
     */
    public void setSequenceSource(String sequenceSource) {
        this.sequenceSource = sequenceSource;
    }

    /**
     * @return the validationMethod
     */
    public String getValidationMethod() {
        return validationMethod;
    }

    /**
     * @param validationMethod the validationMethod to set
     */
    public void setValidationMethod(String validationMethod) {
        this.validationMethod = validationMethod;
    }

    /**
     * @return the score
     */
    public String getScore() {
        return score;
    }

    /**
     * @param score the score to set
     */
    public void setScore(String score) {
        this.score = score;
    }

    /**
     * @return the bamFile
     */
    public String getBamFile() {
        return bamFile;
    }

    /**
     * @param bamFile the bamFile to set
     */
    public void setBamFile(String bamFile) {
        this.bamFile = bamFile;
    }

    /**
     * @return the sequencer
     */
    public String getSequencer() {
        return sequencer;
    }

    /**
     * @param sequencer the sequencer to set
     */
    public void setSequencer(String sequencer) {
        this.sequencer = sequencer;
    }

    /**
     * @return the aminoAcidChange
     */
    public String getAminoAcidChange() {
        return aminoAcidChange;
    }

    /**
     * @param aminoAcidChange the aminoAcidChange to set
     */
    public void setAminoAcidChange(String aminoAcidChange) {
        this.aminoAcidChange = aminoAcidChange;
    }

    /**
     * @return the transcript
     */
    public String getTranscript() {
        return transcript;
    }

    /**
     * @param transcript the transcript to set
     */
    public void setTranscript(String transcript) {
        this.transcript = transcript;
    }

    /**
     * @return the tRefCount
     */
    public String gettRefCount() {
        return tRefCount;
    }

    /**
     * @param tRefCount the tRefCount to set
     */
    public void settRefCount(String tRefCount) {
        this.tRefCount = tRefCount;
    }

    /**
     * @return the tAltCount
     */
    public String gettAltCount() {
        return tAltCount;
    }

    /**
     * @param tAltCount the tAltCount to set
     */
    public void settAltCount(String tAltCount) {
        this.tAltCount = tAltCount;
    }

    /**
     * @return the nRefCount
     */
    public String getnRefCount() {
        return nRefCount;
    }

    /**
     * @param nRefCount the nRefCount to set
     */
    public void setnRefCount(String nRefCount) {
        this.nRefCount = nRefCount;
    }

    /**
     * @return the nAltCount
     */
    public String getnAltCount() {
        return nAltCount;
    }

    /**
     * @param nAltCount the nAltCount to set
     */
    public void setnAltCount(String nAltCount) {
        this.nAltCount = nAltCount;
    }

    /**
     * @return the tTotCov
     */
    public String gettTotCov() {
        return tTotCov;
    }

    /**
     * @param tTotCov the tTotCov to set
     */
    public void settTotCov(String tTotCov) {
        this.tTotCov = tTotCov;
    }

    /**
     * @return the tVarCov
     */
    public String gettVarCov() {
        return tVarCov;
    }

    /**
     * @param tVarCov the tVarCov to set
     */
    public void settVarCov(String tVarCov) {
        this.tVarCov = tVarCov;
    }

    /**
     * @return the nTotCov
     */
    public String getnTotCov() {
        return nTotCov;
    }

    /**
     * @param nTotCov the nTotCov to set
     */
    public void setnTotCov(String nTotCov) {
        this.nTotCov = nTotCov;
    }

    /**
     * @return the nVarCov
     */
    public String getnVarCov() {
        return nVarCov;
    }

    /**
     * @param nVarCov the nVarCov to set
     */
    public void setnVarCov(String nVarCov) {
        this.nVarCov = nVarCov;
    }

    /**
     * @return the tumorDepth
     */
    public String getTumorDepth() {
        return tumorDepth;
    }

    /**
     * @param tumorDepth the tumorDepth to set
     */
    public void setTumorDepth(String tumorDepth) {
        this.tumorDepth = tumorDepth;
    }

    /**
     * @return the tumorVaf
     */
    public String getTumorVaf() {
        return tumorVaf;
    }

    /**
     * @param tumorVaf the tumorVaf to set
     */
    public void setTumorVaf(String tumorVaf) {
        this.tumorVaf = tumorVaf;
    }

    /**
     * @return the normalDepth
     */
    public String getNormalDepth() {
        return normalDepth;
    }

    /**
     * @param normalDepth the normalDepth to set
     */
    public void setNormalDepth(String normalDepth) {
        this.normalDepth = normalDepth;
    }

    /**
     * @return the normalVaf
     */
    public String getNormalVaf() {
        return normalVaf;
    }

    /**
     * @param normalVaf the normalVaf to set
     */
    public void setNormalVaf(String normalVaf) {
        this.normalVaf = normalVaf;
    }

    /**
     * @return the hgvspShort
     */
    public String getHgvspShort() {
        return hgvspShort;
    }

    /**
     * @param hgvspShort the hgvspShort to set
     */
    public void setHgvspShort(String hgvspShort) {
        this.hgvspShort = hgvspShort;
    }

    /**
     * @return the codons
     */
    public String getCodons() {
        return codons;
    }

    /**
     * @param codons the codons to set
     */
    public void setCodons(String codons) {
        this.codons = codons;
    }

    /**
     * @return the swissprot
     */
    public String getSwissprot() {
        return swissprot;
    }

    /**
     * @param swissprot the swissprot to set
     */
    public void setSwissprot(String swissprot) {
        this.swissprot = swissprot;
    }

    /**
     * @return the refseq
     */
    public String getRefseq() {
        return refseq;
    }

    /**
     * @param refseq the refseq to set
     */
    public void setRefseq(String refseq) {
        this.refseq = refseq;
    }

    /**
     * @return the proteinPosition
     */
    public String getProteinPosition() {
        return proteinPosition;
    }

    /**
     * @param proteinPosition the proteinPosition to set
     */
    public void setProteinPosition(String proteinPosition) {
        this.proteinPosition = proteinPosition;
    }

    /**
     * @return the oncotatorCosmicOverlapping
     */
    public String getOncotatorCosmicOverlapping() {
        return oncotatorCosmicOverlapping;
    }

    /**
     * @param oncotatorCosmicOverlapping the oncotatorCosmicOverlapping to set
     */
    public void setOncotatorCosmicOverlapping(String oncotatorCosmicOverlapping) {
        this.oncotatorCosmicOverlapping = oncotatorCosmicOverlapping;
    }

    /**
     * @return the oncotatorDbsnpRs
     */
    public String getOncotatorDbsnpRs() {
        return oncotatorDbsnpRs;
    }

    /**
     * @param oncotatorDbsnpRs the oncotatorDbsnpRs to set
     */
    public void setOncotatorDbsnpRs(String oncotatorDbsnpRs) {
        this.oncotatorDbsnpRs = oncotatorDbsnpRs;
    }

    /**
     * @return the oncotatorDbsnpValStatus
     */
    public String getOncotatorDbsnpValStatus() {
        return oncotatorDbsnpValStatus;
    }

    /**
     * @param oncotatorDbsnpValStatus the oncotatorDbsnpValStatus to set
     */
    public void setOncotatorDbsnpValStatus(String oncotatorDbsnpValStatus) {
        this.oncotatorDbsnpValStatus = oncotatorDbsnpValStatus;
    }

    /**
     * @return the oncotatorProteinChange
     */
    public String getOncotatorProteinChange() {
        return oncotatorProteinChange;
    }

    /**
     * @param oncotatorProteinChange the oncotatorProteinChange to set
     */
    public void setOncotatorProteinChange(String oncotatorProteinChange) {
        this.oncotatorProteinChange = oncotatorProteinChange;
    }

    /**
     * @return the oncotatorVariantClassification
     */
    public String getOncotatorVariantClassification() {
        return oncotatorVariantClassification;
    }

    /**
     * @param oncotatorVariantClassification the oncotatorVariantClassification to set
     */
    public void setOncotatorVariantClassification(String oncotatorVariantClassification) {
        this.oncotatorVariantClassification = oncotatorVariantClassification;
    }

    /**
     * @return the oncotatorGeneSymbol
     */
    public String getOncotatorGeneSymbol() {
        return oncotatorGeneSymbol;
    }

    /**
     * @param oncotatorGeneSymbol the oncotatorGeneSymbol to set
     */
    public void setOncotatorGeneSymbol(String oncotatorGeneSymbol) {
        this.oncotatorGeneSymbol = oncotatorGeneSymbol;
    }

    /**
     * @return the oncotatorRefseqMrnaId
     */
    public String getOncotatorRefseqMrnaId() {
        return oncotatorRefseqMrnaId;
    }

    /**
     * @param oncotatorRefseqMrnaId the oncotatorRefseqMrnaId to set
     */
    public void setOncotatorRefseqMrnaId(String oncotatorRefseqMrnaId) {
        this.oncotatorRefseqMrnaId = oncotatorRefseqMrnaId;
    }

    /**
     * @return the oncotatorRefseqProtId
     */
    public String getOncotatorRefseqProtId() {
        return oncotatorRefseqProtId;
    }

    /**
     * @param oncotatorRefseqProtId the oncotatorRefseqProtId to set
     */
    public void setOncotatorRefseqProtId(String oncotatorRefseqProtId) {
        this.oncotatorRefseqProtId = oncotatorRefseqProtId;
    }

    /**
     * @return the oncotatorUniprotEntryName
     */
    public String getOncotatorUniprotEntryName() {
        return oncotatorUniprotEntryName;
    }

    /**
     * @param oncotatorUniprotEntryName the oncotatorUniprotEntryName to set
     */
    public void setOncotatorUniprotEntryName(String oncotatorUniprotEntryName) {
        this.oncotatorUniprotEntryName = oncotatorUniprotEntryName;
    }

    /**
     * @return the oncotatorUniprotAccession
     */
    public String getOncotatorUniprotAccession() {
        return oncotatorUniprotAccession;
    }

    /**
     * @param oncotatorUniprotAccession the oncotatorUniprotAccession to set
     */
    public void setOncotatorUniprotAccession(String oncotatorUniprotAccession) {
        this.oncotatorUniprotAccession = oncotatorUniprotAccession;
    }

    /**
     * @return the oncotatorCodonChange
     */
    public String getOncotatorCodonChange() {
        return oncotatorCodonChange;
    }

    /**
     * @param oncotatorCodonChange the oncotatorCodonChange to set
     */
    public void setOncotatorCodonChange(String oncotatorCodonChange) {
        this.oncotatorCodonChange = oncotatorCodonChange;
    }

    /**
     * @return the oncotatorTranscriptChange
     */
    public String getOncotatorTranscriptChange() {
        return oncotatorTranscriptChange;
    }

    /**
     * @param oncotatorTranscriptChange the oncotatorTranscriptChange to set
     */
    public void setOncotatorTranscriptChange(String oncotatorTranscriptChange) {
        this.oncotatorTranscriptChange = oncotatorTranscriptChange;
    }

    /**
     * @return the oncotatorExonAffected
     */
    public String getOncotatorExonAffected() {
        return oncotatorExonAffected;
    }

    /**
     * @param oncotatorExonAffected the oncotatorExonAffected to set
     */
    public void setOncotatorExonAffected(String oncotatorExonAffected) {
        this.oncotatorExonAffected = oncotatorExonAffected;
    }

    /**
     * @return the oncotatorProteinPosStart
     */
    public String getOncotatorProteinPosStart() {
        return oncotatorProteinPosStart;
    }

    /**
     * @param oncotatorProteinPosStart the oncotatorProteinPosStart to set
     */
    public void setOncotatorProteinPosStart(String oncotatorProteinPosStart) {
        this.oncotatorProteinPosStart = oncotatorProteinPosStart;
    }

    /**
     * @return the oncotatorProteinPosEnd
     */
    public String getOncotatorProteinPosEnd() {
        return oncotatorProteinPosEnd;
    }

    /**
     * @param oncotatorProteinPosEnd the oncotatorProteinPosEnd to set
     */
    public void setOncotatorProteinPosEnd(String oncotatorProteinPosEnd) {
        this.oncotatorProteinPosEnd = oncotatorProteinPosEnd;
    }

    /**
     * @return the oncotatorProteinChangeBe
     */
    public String getOncotatorProteinChangeBe() {
        return oncotatorProteinChangeBe;
    }

    /**
     * @param oncotatorProteinChangeBe the oncotatorProteinChangeBe to set
     */
    public void setOncotatorProteinChangeBe(String oncotatorProteinChangeBe) {
        this.oncotatorProteinChangeBe = oncotatorProteinChangeBe;
    }

    /**
     * @return the oncotatorVariantClassificationBe
     */
    public String getOncotatorVariantClassificationBe() {
        return oncotatorVariantClassificationBe;
    }

    /**
     * @param oncotatorVariantClassificationBe the oncotatorVariantClassificationBe to set
     */
    public void setOncotatorVariantClassificationBe(String oncotatorVariantClassificationBe) {
        this.oncotatorVariantClassificationBe = oncotatorVariantClassificationBe;
    }

    /**
     * @return the oncotatorGeneSymbolBe
     */
    public String getOncotatorGeneSymbolBe() {
        return oncotatorGeneSymbolBe;
    }

    /**
     * @param oncotatorGeneSymbolBe the oncotatorGeneSymbolBe to set
     */
    public void setOncotatorGeneSymbolBe(String oncotatorGeneSymbolBe) {
        this.oncotatorGeneSymbolBe = oncotatorGeneSymbolBe;
    }

    /**
     * @return the oncotatorRefseqMrnaIdBe
     */
    public String getOncotatorRefseqMrnaIdBe() {
        return oncotatorRefseqMrnaIdBe;
    }

    /**
     * @param oncotatorRefseqMrnaIdBe the oncotatorRefseqMrnaIdBe to set
     */
    public void setOncotatorRefseqMrnaIdBe(String oncotatorRefseqMrnaIdBe) {
        this.oncotatorRefseqMrnaIdBe = oncotatorRefseqMrnaIdBe;
    }

    /**
     * @return the oncotatorRefseqProtIdBe
     */
    public String getOncotatorRefseqProtIdBe() {
        return oncotatorRefseqProtIdBe;
    }

    /**
     * @param oncotatorRefseqProtIdBe the oncotatorRefseqProtIdBe to set
     */
    public void setOncotatorRefseqProtIdBe(String oncotatorRefseqProtIdBe) {
        this.oncotatorRefseqProtIdBe = oncotatorRefseqProtIdBe;
    }

    /**
     * @return the oncotatorUniprotEntryNameBe
     */
    public String getOncotatorUniprotEntryNameBe() {
        return oncotatorUniprotEntryNameBe;
    }

    /**
     * @param oncotatorUniprotEntryNameBe the oncotatorUniprotEntryNameBe to set
     */
    public void setOncotatorUniprotEntryNameBe(String oncotatorUniprotEntryNameBe) {
        this.oncotatorUniprotEntryNameBe = oncotatorUniprotEntryNameBe;
    }

    /**
     * @return the oncotatorUniprotAccessionBe
     */
    public String getOncotatorUniprotAccessionBe() {
        return oncotatorUniprotAccessionBe;
    }

    /**
     * @param oncotatorUniprotAccessionBe the oncotatorUniprotAccessionBe to set
     */
    public void setOncotatorUniprotAccessionBe(String oncotatorUniprotAccessionBe) {
        this.oncotatorUniprotAccessionBe = oncotatorUniprotAccessionBe;
    }

    /**
     * @return the oncotatorCodonChangeBe
     */
    public String getOncotatorCodonChangeBe() {
        return oncotatorCodonChangeBe;
    }

    /**
     * @param oncotatorCodonChangeBe the oncotatorCodonChangeBe to set
     */
    public void setOncotatorCodonChangeBe(String oncotatorCodonChangeBe) {
        this.oncotatorCodonChangeBe = oncotatorCodonChangeBe;
    }

    /**
     * @return the oncotatorTranscriptChangeBe
     */
    public String getOncotatorTranscriptChangeBe() {
        return oncotatorTranscriptChangeBe;
    }

    /**
     * @param oncotatorTranscriptChangeBe the oncotatorTranscriptChangeBe to set
     */
    public void setOncotatorTranscriptChangeBe(String oncotatorTranscriptChangeBe) {
        this.oncotatorTranscriptChangeBe = oncotatorTranscriptChangeBe;
    }

    /**
     * @return the oncotatorExonAffectedBe
     */
    public String getOncotatorExonAffectedBe() {
        return oncotatorExonAffectedBe;
    }

    /**
     * @param oncotatorExonAffectedBe the oncotatorExonAffectedBe to set
     */
    public void setOncotatorExonAffectedBe(String oncotatorExonAffectedBe) {
        this.oncotatorExonAffectedBe = oncotatorExonAffectedBe;
    }

    /**
     * @return the oncotatorProteinPosStartBe
     */
    public String getOncotatorProteinPosStartBe() {
        return oncotatorProteinPosStartBe;
    }

    /**
     * @param oncotatorProteinPosStartBe the oncotatorProteinPosStartBe to set
     */
    public void setOncotatorProteinPosStartBe(String oncotatorProteinPosStartBe) {
        this.oncotatorProteinPosStartBe = oncotatorProteinPosStartBe;
    }

    /**
     * @return the oncotatorProteinPosEndBe
     */
    public String getOncotatorProteinPosEndBe() {
        return oncotatorProteinPosEndBe;
    }

    /**
     * @param oncotatorProteinPosEndBe the oncotatorProteinPosEndBe to set
     */
    public void setOncotatorProteinPosEndBe(String oncotatorProteinPosEndBe) {
        this.oncotatorProteinPosEndBe = oncotatorProteinPosEndBe;
    }

    /**
     * @return the maFimpact
     */
    public String getMaFimpact() {
        return maFimpact;
    }

    /**
     * @param maFimpact the maFimpact to set
     */
    public void setMaFimpact(String maFimpact) {
        this.maFimpact = maFimpact;
    }

    /**
     * @return the maFis
     */
    public String getMaFis() {
        return maFis;
    }

    /**
     * @param maFis the maFis to set
     */
    public void setMaFis(String maFis) {
        this.maFis = maFis;
    }

    /**
     * @return the maLinkVar
     */
    public String getMaLinkVar() {
        return maLinkVar;
    }

    /**
     * @param maLinkVar the maLinkVar to set
     */
    public void setMaLinkVar(String maLinkVar) {
        this.maLinkVar = maLinkVar;
    }

    /**
     * @return the maLinkMsa
     */
    public String getMaLinkMsa() {
        return maLinkMsa;
    }

    /**
     * @param maLinkMsa the maLinkMsa to set
     */
    public void setMaLinkMsa(String maLinkMsa) {
        this.maLinkMsa = maLinkMsa;
    }

    /**
     * @return the maLinkPdb
     */
    public String getMaLinkPdb() {
        return maLinkPdb;
    }

    /**
     * @param maLinkPdb the maLinkPdb to set
     */
    public void setMaLinkPdb(String maLinkPdb) {
        this.maLinkPdb = maLinkPdb;
    }

    /**
     * @return the maProteinChange
     */
    public String getMaProteinChange() {
        return maProteinChange;
    }

    /**
     * @param maProteinChange the maProteinChange to set
     */
    public void setMaProteinChange(String maProteinChange) {
        this.maProteinChange = maProteinChange;
    }

    /**
     * @return the MAF staging data map (column -> field)
     */
    public Map<String, String> getMafStagingDataMap() {
        Map<String, String> map = new HashMap<>();
        map.put("Hugo_Symbol", "hugoSymbol");
        map.put("Entrez_Gene_Id", "entrezGeneId");
        map.put("Center", "center");
        map.put("NCBI_Build", "ncbiBuild");
        map.put("Chromosome", "chromosome");
        map.put("Start_Position", "startPosition");
        map.put("End_Position", "endPosition");
        map.put("Strand", "strand");
        map.put("Variant_Classification", "variantClassification");
        map.put("Variant_Type", "variantType");
        map.put("Reference_Allele", "referenceAllele");
        map.put("Tumor_Seq_Allele1", "tumorSeqAllele1");
        map.put("Tumor_Seq_Allele2", "tumorSeqAllele2");
        map.put("dbSNP_RS", "dbsnpRs");
        map.put("dbSNP_Val_Status", "dbsnpValStatus");
        map.put("Tumor_Sample_Barcode", "tumorSampleBarcode");
        map.put("Matched_Norm_Sample_Barcode", "matchedNormSampleBarcode");
        map.put("Match_Norm_Seq_Allele1", "matchNormSeqAllele1");
        map.put("Match_Norm_Seq_Allele2", "matchNormSeqAllele2");
        map.put("Tumor_Validation_Allele1", "tumorValidationAllele1");
        map.put("Tumor_Validation_Allele2", "tumorValidationAllele2");
        map.put("Match_Norm_Validation_Allele1", "matchNormValidationAllele1");
        map.put("Match_Norm_Validation_Allele2", "matchNormValidationAllele2");
        map.put("Verification_Status", "verificationStatus");
        map.put("Validation_Status", "validationStatus");
        map.put("Mutation_Status", "mutationStatus");
        map.put("Sequencing_Phase", "sequencingPhase");
        map.put("Sequence_Source", "sequenceSource");
        map.put("Validation_Method", "validationMethod");
        map.put("Score", "score");
        map.put("BAM_File", "bamFile");
        map.put("Sequencer", "sequencer");
        map.put("Amino_Acid_Change", "aminoAcidChange");
        map.put("Transcript", "transcript");
        map.put("t_ref_count", "tRefCount");
        map.put("t_alt_count", "tAltCount");
        map.put("n_ref_count", "nRefCount");
        map.put("n_alt_count", "nAltCount");
        map.put("TTotCov", "tTotCov");
        map.put("TVarCov", "tVarCov");
        map.put("NTotCov", "nTotCov");
        map.put("NVarCov", "nVarCov");
        map.put("normal_depth", "normalDepth");
        map.put("normal_vaf", "normalVaf");
        map.put("HGVSp_Short", "hgvspShort");
        map.put("Codons", "codons");
        map.put("SWISSPROT", "swissprot");
        map.put("RefSeq", "refseq");
        map.put("Protein_position", "proteinPosition");
        map.put("ONCOTATOR_COSMIC_OVERLAPPING", "oncotatorCosmicOverlapping");
        map.put("ONCOTATOR_DBSNP_RS", "oncotatorDbsnpRs");
        map.put("ONCOTATOR_DBSNP_VAL_STATUS", "oncotatorDbsnpValStatus");
        map.put("ONCOTATOR_PROTEIN_CHANGE", "oncotatorProteinChange");
        map.put("ONCOTATOR_VARIANT_CLASSIFICATION", "oncotatorVariantClassification");
        map.put("ONCOTATOR_GENE_SYMBOL", "oncotatorGeneSymbol");
        map.put("ONCOTATOR_REFSEQ_MRNA_ID", "oncotatorRefseqMrnaId");
        map.put("ONCOTATOR_REFSEQ_PROT_ID", "oncotatorRefseqProtId");
        map.put("ONCOTATOR_UNIPROT_ENTRY_NAME", "oncotatorUniprotEntryName");
        map.put("ONCOTATOR_UNIPROT_ACCESSION", "oncotatorUniprotAccession");
        map.put("ONCOTATOR_CODON_CHANGE", "oncotatorCodonChange");
        map.put("ONCOTATOR_TRANSCRIPT_CHANGE", "oncotatorTranscriptChange");
        map.put("ONCOTATOR_EXON_AFFECTED", "oncotatorExonAffected");
        map.put("ONCOTATOR_PROTEIN_POS_START", "oncotatorProteinPosStart");
        map.put("ONCOTATOR_PROTEIN_POS_END", "oncotatorProteinPosEnd");
        map.put("ONCOTATOR_PROTEIN_CHANGE_BEST_EFFECT", "oncotatorProteinChangeBe");
        map.put("ONCOTATOR_VARIANT_CLASSIFICATION_BEST_EFFECT", "oncotatorVariantClassificationBe");
        map.put("ONCOTATOR_GENE_SYMBOL_BEST_EFFECT", "oncotatorGeneSymbolBe");
        map.put("ONCOTATOR_REFSEQ_MRNA_ID_BEST_EFFECT", "oncotatorRefseqMrnaIdBe");
        map.put("ONCOTATOR_REFSEQ_PROT_ID_BEST_EFFECT", "oncotatorRefseqProtIdBe");
        map.put("ONCOTATOR_UNIPROT_ENTRY_NAME_BEST_EFFECT", "oncotatorUniprotEntryNameBe");
        map.put("ONCOTATOR_UNIPROT_ACCESSION_BEST_EFFECT", "oncotatorUniprotAccessionBe");
        map.put("ONCOTATOR_CODON_CHANGE_BEST_EFFECT", "oncotatorCodonChangeBe");
        map.put("ONCOTATOR_TRANSCRIPT_CHANGE_BEST_EFFECT", "oncotatorTranscriptChangeBe");
        map.put("ONCOTATOR_EXON_AFFECTED_BEST_EFFECT", "oncotatorExonAffectedBe");
        map.put("ONCOTATOR_PROTEIN_POS_START_BEST_EFFECT", "oncotatorProteinPosStartBe");
        map.put("ONCOTATOR_PROTEIN_POS_END_BEST_EFFECT", "oncotatorProteinPosEndBe");
        map.put("MA:FImpact", "maFimpact");
        map.put("MA:FIS", "maFis");
        map.put("MA:link.var", "maLinkVar");
        map.put("MA:link.MSA", "maLinkMsa");
        map.put("MA:link.PDB", "maLinkPdb");
        map.put("MA:protein.change", "maProteinChange");
        
        // these columns have multiple possible header names
        map.put("t_depth", "tumorDepth");
        map.put("t_vaf", "tumorVaf");        
        map.put("tumor_depth", "tumorDepth");
        map.put("tumor_vaf", "tumorVaf");
        
        return map;
    }    

}

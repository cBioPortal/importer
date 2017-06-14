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

package org.cbio.portal.pipelines.importer.config.step;

import org.mskcc.cbio.model.SampleList;
import org.cbio.portal.pipelines.importer.config.listener.CaseListListener;
import org.cbio.portal.pipelines.importer.config.reader.CaseListReader;
import org.cbio.portal.pipelines.importer.config.writer.CaseListWriter;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.item.*;

import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
public class CaseListStep {
    
    @Value("${chunk.interval}")
    private int chunkInterval;
    
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Bean
    public Step importCaseLists() {
        return stepBuilderFactory.get("importCaseLists")
                .<SampleList, SampleList> chunk(chunkInterval)
                .reader(caseListReader())
                .writer(caseListWriter())
                .listener(caseListListener())
                .build();
    }
    
    /***************************************************************************
     * Case list listener, reader, and writer.
     **************************************************************************/
    
    @Bean
    public StepExecutionListener caseListListener() {
        return new CaseListListener();
    }
    
    @Bean
    @StepScope
    public ItemStreamReader<SampleList> caseListReader() {
        return new CaseListReader();
    }
    
    @Bean
    @StepScope
    public ItemStreamWriter<SampleList> caseListWriter() {
        return new CaseListWriter();
    }
    
}

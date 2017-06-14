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

package org.cbio.portal.pipelines.importer;

import org.cbio.portal.pipelines.importer.config.BatchConfiguration;

import java.io.*;
import java.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.logging.*;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;

/**
 *
 * @author ochoaa
 */
@SpringBootApplication
public class ImporterPipeline {

    private static final Log LOG = LogFactory.getLog(ImporterPipeline.class);   
    
    private static Options getOptions(String[] args) {
        Options gnuOptions = new Options();
        gnuOptions.addOption("h", "help", false, "shows this help document and quits.")
            .addOption("i", "import_study", true, "Cancer study directory to import")
                .addOption("d", "delete_study", true, "Cancer study identifier for deleting study");
        return gnuOptions;
    }

    private static void help(Options gnuOptions, int exitStatus) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("ImporterPipeline", gnuOptions);
        System.exit(exitStatus);
    }

    private static void launchImporterJob(String[] args, String stagingDirectory) throws Exception {

        SpringApplication app = new SpringApplication(ImporterPipeline.class);
        ConfigurableApplicationContext ctx = app.run(args);
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        
        Job batchImporterJob = ctx.getBean(BatchConfiguration.BATCH_STUDY_IMPORTER_JOB, Job.class);        

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("stagingDirectory", stagingDirectory)
                .addDate("date", new Date())
                .toJobParameters();
        
        JobExecution jobExecution = jobLauncher.run(batchImporterJob, jobParameters);
        if (jobExecution.getExitStatus().getExitCode().equals("STOPPED")) {
            LOG.error("Error importing cancer study.");
        }
        ctx.close();
    }
    
    private static void launchDeleteStudyJob(String[] args, String cancerStudyIdentifier) throws Exception {
        SpringApplication app = new SpringApplication(ImporterPipeline.class);
        ConfigurableApplicationContext ctx = app.run(args);
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        
        Job deleteStudyJob = ctx.getBean(BatchConfiguration.DELETE_CANCER_STUDY_JOB, Job.class);
        
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("cancerStudyIdentifier", cancerStudyIdentifier)
                .toJobParameters();
        
        JobExecution jobExecution = jobLauncher.run(deleteStudyJob, jobParameters);
        ctx.close();
    }

    public static void main(String[] args) throws Exception {
        Options gnuOptions = ImporterPipeline.getOptions(args);
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(gnuOptions, args);
        if (commandLine.hasOption("h") ||
            (!commandLine.hasOption("i") && !commandLine.hasOption("d"))) {
            help(gnuOptions, 0);
        }
        
        if (commandLine.hasOption("d")) {
            String cancerStudyIdentifier = commandLine.getOptionValue("d");
            launchDeleteStudyJob(args, cancerStudyIdentifier);
        }
        
        if (commandLine.hasOption("i")) {
            String stagingDirectory = commandLine.getOptionValue("i");
            if (!(new File(stagingDirectory).exists())) {
                LOG.error("Staging directory does not exist - please check argument: " + stagingDirectory);
                System.exit(2);
            }
            launchImporterJob(args, stagingDirectory);
        }
    }
    
}

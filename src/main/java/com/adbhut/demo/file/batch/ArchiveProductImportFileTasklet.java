package com.adbhut.demo.file.batch;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;

public class ArchiveProductImportFileTasklet implements Tasklet {

    @Value("${processed.directory:c:/Users/adbhu/Desktop/csv/processed}")
    private String processedDirectory;

    private String inputFile;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Make our destination directory and copy our input file to it
        File archiveDir = new File(processedDirectory);
        FileUtils.forceMkdir(archiveDir);
        FileUtils.copyFileToDirectory(new File(inputFile), archiveDir);
        System.out.println("Done");
        // We're done...
        return RepeatStatus.FINISHED;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

}

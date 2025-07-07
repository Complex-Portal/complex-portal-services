package uk.ac.ebi.complex.service.interactions.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.interactions.config.FileConfiguration;
import uk.ac.ebi.complex.service.interactions.logging.ReportWriter;
import uk.ac.ebi.complex.service.interactions.model.ComplexIntactCoverage;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Log4j
@RequiredArgsConstructor
public class ComplexIntactCoverageWriter implements ItemWriter<ComplexIntactCoverage>, ItemStream {

    protected final FileConfiguration fileConfiguration;

    private ReportWriter proteinPairsWithNoIntactInteractions;
    private ReportWriter proteinPairsWithIntactInteractions;

    @Override
    public void write(List<? extends ComplexIntactCoverage> items) throws IOException {
        for (ComplexIntactCoverage item : items) {
            for (ComplexIntactCoverage.ProteinPairScore proteinPairScore: item.getProteinPairScores()) {
                if (proteinPairScore.getIntactScore() == null) {
                    proteinPairsWithNoIntactInteractions.writeLine(new String[]{
                            item.getComplexAc(),
                            proteinPairScore.getProteinA(),
                            proteinPairScore.getProteinB()
                    });
                } else {
                    proteinPairsWithIntactInteractions.writeLine(new String[]{
                            item.getComplexAc(),
                            proteinPairScore.getProteinA(),
                            proteinPairScore.getProteinB(),
                            String.valueOf(proteinPairScore.getIntactScore())
                    });
                }
            }
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be opened", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.proteinPairsWithNoIntactInteractions.flush();
            this.proteinPairsWithIntactInteractions.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.proteinPairsWithNoIntactInteractions.flush();
            this.proteinPairsWithNoIntactInteractions.close();
            this.proteinPairsWithIntactInteractions.flush();
            this.proteinPairsWithIntactInteractions.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    protected void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        File noInteractionsFile = new File(reportDirectory, "no_interactions" + extension);
        this.proteinPairsWithNoIntactInteractions = new ReportWriter(noInteractionsFile, separator, header,
                new String[]{"complex_id", "protein_a", "protein_b"});
        File withInteractionsFile = new File(reportDirectory, "with_interactions" + extension);
        this.proteinPairsWithIntactInteractions = new ReportWriter(withInteractionsFile, separator, header,
                new String[]{"complex_id", "protein_a", "protein_b", "mi_score"});
    }
}

package uk.ac.ebi.complex.service.processor;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.ComplexFinderResult;
import uk.ac.ebi.complex.service.config.ComplexServiceConfiguration;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.logging.ReportWriter;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.model.UniplexComplex;
import uk.ac.ebi.complex.service.service.IntactComplexService;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class UniplexClusterProcessor implements ItemProcessor<UniplexCluster, UniplexComplex>, ItemStream {

    private static final Log LOG = LogFactory.getLog(UniplexClusterProcessor.class);

    private IntactComplexService intactComplexService;
    private ComplexServiceConfiguration config;

    private ReportWriter exactMatchesReportWriter;
    private ReportWriter multipleExactMatchesReportWriter;
    private ReportWriter partialMatchesReportWriter;
    private ReportWriter noMatchesReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public UniplexComplex process(UniplexCluster item) throws Exception {
        try {
            ComplexFinderResult<IntactComplex> complexFinderResult = intactComplexService
                    .findComplexWithMatchingProteins(item.getUniprotAcs());

            if (!complexFinderResult.getExactMatches().isEmpty()) {
                if (complexFinderResult.getExactMatches().size() > 1) {
                    for (ComplexFinderResult.ExactMatch<IntactComplex> complexMatch : complexFinderResult.getExactMatches()) {
                        multipleExactMatchesReportWriter.write(
                                complexMatch.getMatchType(),
                                item.getClusterIds(),
                                item.getClusterConfidence(),
                                item.getUniprotAcs(),
                                complexMatch.getComplexAc());
                    }
                    LOG.warn("Clusters " +
                            String.join(",", item.getClusterIds()) +
                            " matched exactly to multiple complexes: " +
                            complexFinderResult.getExactMatches()
                                    .stream()
                                    .map(ComplexFinderResult.ExactMatch::getComplexAc)
                                    .collect(Collectors.joining(",")));
                    return null;
                } else {
                    ComplexFinderResult.ExactMatch<IntactComplex> complexMatch = complexFinderResult.getExactMatches().iterator().next();
                    exactMatchesReportWriter.write(
                            complexMatch.getMatchType(),
                            item.getClusterIds(),
                            item.getClusterConfidence(),
                            item.getUniprotAcs(),
                            complexMatch.getComplexAc());
                    return new UniplexComplex(item, complexMatch.getComplex());
                }
            } else {
                // At the moment we only consider partial matches where the predicted complex is a subset of a curated complex.
                List<ComplexFinderResult.PartialMatch<IntactComplex>> subsetMatches = complexFinderResult.getPartialMatches()
                        .stream()
                        .filter(match -> ComplexFinderResult.MatchType.PARTIAL_MATCH_SUBSET_OF_COMPLEX.equals(match.getMatchType()))
                        .collect(Collectors.toList());
                if (!subsetMatches.isEmpty()) {
                    for (ComplexFinderResult.PartialMatch<IntactComplex> complexMatch : subsetMatches) {
                        partialMatchesReportWriter.write(
                                complexMatch.getMatchType(),
                                item.getClusterIds(),
                                item.getClusterConfidence(),
                                item.getUniprotAcs(),
                                complexMatch.getComplexAc());
                    }
                } else {
                    noMatchesReportWriter.write(
                            item.getClusterIds(),
                            item.getClusterConfidence(),
                            item.getUniprotAcs());
                }
                return new UniplexComplex(item, null);
            }
        } catch (Exception e) {
            LOG.error("Error finding complex matches for uniplex clusters: " + String.join(",", item.getClusterIds()), e);
            errorReportWriter.write(item.getClusterIds(), e.getMessage());
            return null;
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file  writer could not be opened", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.exactMatchesReportWriter.flush();
            this.multipleExactMatchesReportWriter.flush();
            this.partialMatchesReportWriter.flush();
            this.noMatchesReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.exactMatchesReportWriter.close();
            this.multipleExactMatchesReportWriter.close();
            this.partialMatchesReportWriter.close();
            this.noMatchesReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    private void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(config.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + config.getReportDirectory());
        }

        String sep = config.getSeparator();
        boolean header = config.isHeader();
        this.exactMatchesReportWriter = new ProcessReportWriter(new File(reportDirectory, "exact_matches.csv"), sep, header);
        this.multipleExactMatchesReportWriter = new ProcessReportWriter(new File(reportDirectory, "multiple_exact_matches.csv"), sep, header);
        this.partialMatchesReportWriter = new ProcessReportWriter(new File(reportDirectory, "partial_matches.csv"), sep, header);
        this.noMatchesReportWriter = new ProcessReportWriter(new File(reportDirectory, "no_matches.csv"), sep, header);
        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "process_errors.csv"), sep, header);
    }
}

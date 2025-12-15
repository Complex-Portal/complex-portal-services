package uk.ac.ebi.complex.service.batch.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.batch.config.AppProperties;
import uk.ac.ebi.complex.service.finder.ComplexFinder;
import uk.ac.ebi.complex.service.finder.ComplexFinderOptions;
import uk.ac.ebi.complex.service.finder.ComplexFinderResult;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.batch.model.ComplexCluster;
import uk.ac.ebi.complex.service.batch.model.ComplexWithMatches;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class ComplexImportBatchProcessor<T, R extends ComplexToImport<T>> extends AbstractBatchProcessor<R, ComplexWithMatches<T, R>> {

    private final AppProperties appProperties;
    private final ComplexFinder complexFinder;
    private final ComplexManager<T, R> complexManager;

    private ProcessReportWriter exactMatchesReportWriter;
    private ProcessReportWriter multipleExactMatchesReportWriter;
    private ProcessReportWriter partialMatchesReportWriter;
    private ProcessReportWriter noMatchesReportWriter;
    private ErrorsReportWriter errorReportWriter;

    private Map<String, Set<String>> matchesAlreadyProcessed;

    @Override
    public ComplexWithMatches<T, R> process(R item) throws Exception {
        try {
            ComplexFinderOptions complexFinderOptions = ComplexFinderOptions.builder()
                    .checkPredictedComplexes(true)
                    .checkAnyStatusForExactMatches(true)
                    .checkPartialMatches(true)
                    .build();

            Set<String> matchesAlreadyFound = new HashSet<>();
            for (String id: item.getComplexIds()) {
                if (matchesAlreadyProcessed.containsKey(id)) {
                    matchesAlreadyFound.addAll(matchesAlreadyProcessed.get(id));
                }
            }

            ComplexFinderResult<IntactComplex> complexFinderResult;
            if (matchesAlreadyFound.isEmpty()) {
                complexFinderResult = this.complexFinder.findComplexWithMatchingProteins(
                        item.getProteinIds(), complexFinderOptions);
            } else {
                complexFinderResult = this.complexFinder.findComplexWithMatchingProteinsInComplexes(
                        item.getProteinIds(), matchesAlreadyFound, complexFinderOptions);
            }

            List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatchesToConsider = complexFinderResult.getExactMatches()
                    .stream()
                    .filter(match -> {
                        if (LifeCycleStatus.RELEASED.equals(match.getComplex().getStatus()) ||
                                LifeCycleStatus.READY_FOR_RELEASE.equals(match.getComplex().getStatus())) {
                            return true;
                        }
                        // We only consider released or ready for released complexes, or those complexes already
                        // with the matching id
                        return complexManager.doesComplexHasIdentityXref(item, match.getComplex());
                    })
                    .collect(Collectors.toList());

            List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatchesNotOnHold = exactMatchesToConsider
                    .stream()
                    .filter(match -> LifeCycleStatus.RELEASED.equals(match.getComplex().getStatus()) ||
                            LifeCycleStatus.READY_FOR_RELEASE.equals(match.getComplex().getStatus()))
                    .collect(Collectors.toList());

            if (exactMatchesNotOnHold.size() == 1) {
                logSingleExactMatch(item, exactMatchesNotOnHold);
            } else if (exactMatchesNotOnHold.size() > 1) {
                logMultipleExactMatches(item, exactMatchesNotOnHold);
                return null;
            }

            if (exactMatchesToConsider.isEmpty() && complexFinderResult.getPartialMatches().isEmpty()) {
                logNoMatches(item);
            }

            List<ComplexFinderResult.PartialMatch<IntactComplex>> subsetOfComplexes = complexFinderResult.getPartialMatches()
                    .stream()
                    .filter(match -> ComplexFinderResult.MatchType.PARTIAL_MATCH_SUBSET_OF_COMPLEX.equals(match.getMatchType()))
                    .filter(match -> LifeCycleStatus.RELEASED.equals(match.getComplex().getStatus()) ||
                            LifeCycleStatus.READY_FOR_RELEASE.equals(match.getComplex().getStatus()))
                    .collect(Collectors.toList());

            if (!subsetOfComplexes.isEmpty()) {
                logPartialMatches(item, ComplexFinderResult.MatchType.PARTIAL_MATCH_SUBSET_OF_COMPLEX, subsetOfComplexes);
            }

            List<ComplexFinderResult.PartialMatch<IntactComplex>> supersetOfComplexes = complexFinderResult.getPartialMatches()
                    .stream()
                    .filter(match -> ComplexFinderResult.MatchType.PARTIAL_MATCH_PROTEINS_MISSING_IN_COMPLEX.equals(match.getMatchType()))
                    .filter(match -> LifeCycleStatus.RELEASED.equals(match.getComplex().getStatus()) ||
                            LifeCycleStatus.READY_FOR_RELEASE.equals(match.getComplex().getStatus()))
                    .collect(Collectors.toList());

            Collection<List<IntactComplex>> clusterGroupComplexes;
            if (!supersetOfComplexes.isEmpty()) {
                logPartialMatches(item, ComplexFinderResult.MatchType.PARTIAL_MATCH_PROTEINS_MISSING_IN_COMPLEX, supersetOfComplexes);
                clusterGroupComplexes = findMatchesForComplexCluster(
                        Set.copyOf(item.getProteinIds()),
                        supersetOfComplexes);
            } else {
                clusterGroupComplexes = List.of();
            }

            return new ComplexWithMatches<>(
                    item,
                    exactMatchesToConsider.stream().map(ComplexFinderResult.ExactMatch::getComplex).collect(Collectors.toList()),
                    subsetOfComplexes.stream().map(ComplexFinderResult.PartialMatch::getComplex).collect(Collectors.toList()),
                    clusterGroupComplexes);

        } catch (Exception e) {
            log.error("Error finding complex matches for complexes with ids: " + String.join(",", item.getComplexIds()), e);
            errorReportWriter.write(item.getComplexIds(), e.getMessage());
            return null;
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

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String sep = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        matchesAlreadyProcessed = new HashMap<>();
        if (appProperties.isReuseExistingFiles()) {
            readComplexIdsFromFile(new File(reportDirectory, "exact_matches" + extension));
            readComplexIdsFromFile(new File(reportDirectory, "multiple_exact_matches" + extension));
            readComplexIdsFromFile(new File(reportDirectory, "partial_matches" + extension));
        }

        this.exactMatchesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "exact_matches" + extension), sep, header, ProcessReportWriter.EXACT_MATCH_HEADER_LINE);
        this.multipleExactMatchesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "multiple_exact_matches" + extension), sep, header, ProcessReportWriter.MULTIPLE_EXACT_MATCHES_HEADER_LINE);
        this.partialMatchesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "partial_matches" + extension), sep, header, ProcessReportWriter.PARTIAL_MATCHES_HEADER_LINE);
        this.noMatchesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "no_matches" + extension), sep, header, ProcessReportWriter.NO_MATCH_HEADER_LINE);
        this.errorReportWriter = new ErrorsReportWriter(
                new File(reportDirectory, "process_errors" + extension), sep, header);
    }

    private Collection<List<IntactComplex>> findMatchesForComplexCluster(
            Set<String> proteins,
            List<ComplexFinderResult.PartialMatch<IntactComplex>> matches) {

        ComplexCluster emptyCluster = new ComplexCluster(
                List.of(),
                Set.of(),
                proteins);

        List<ComplexCluster> complexClusters = checkAndMergeMatches(emptyCluster, matches);

        // We do not want complex clusters that are a superset of another complex cluster, so we sort them by size,
        // and process them from smallest to largest, adding only those clusters for which we already do not have
        // a cluster with a subset of the complexes
        complexClusters.sort(Comparator.comparingInt(xs -> xs.getComplexAcs().size()));
        List<ComplexCluster> filteredClusters = new ArrayList<>();
        for (ComplexCluster clusterA : complexClusters) {
            if (filteredClusters.stream().noneMatch(clusterB -> doesClusterContainAnotherCluster(clusterA, clusterB))) {
                filteredClusters.add(clusterA);
            }
        }

        return filteredClusters.stream()
                .map(cluster -> matches.stream()
                        .filter(match -> cluster.getComplexAcs().contains(match.getComplexAc()))
                        .map(ComplexFinderResult.PartialMatch::getComplex)
                        .collect(Collectors.toList()))
                .distinct()
                .collect(Collectors.toList());
    }

    private boolean doesClusterContainAnotherCluster(ComplexCluster clusterA, ComplexCluster clusterB) {
        return clusterA.getComplexAcs().containsAll(clusterB.getComplexAcs());
    }

    private List<ComplexCluster> checkAndMergeMatches(
            ComplexCluster complexCluster,
            List<ComplexFinderResult.PartialMatch<IntactComplex>> matchesToCheck) {

        Set<String> proteinsFromAllMatchesToCheck = matchesToCheck.stream()
                .flatMap(match -> match.getMatchingProteins().stream())
                .collect(Collectors.toSet());

        if (proteinsFromAllMatchesToCheck.size() < complexCluster.getMissingProteins().size() ||
                !proteinsFromAllMatchesToCheck.containsAll(complexCluster.getMissingProteins())) {
            return new ArrayList<>();
        }

        List<ComplexCluster> complexClusters = new ArrayList<>();

        for (int i = 0; i < matchesToCheck.size(); i++) {
            ComplexFinderResult.PartialMatch<IntactComplex> matchToCheck = matchesToCheck.get(i);
            ComplexCluster mergedComplexCluster = checkAndMergeMatchIntoCluster(
                    complexCluster,
                    matchToCheck);
            if (mergedComplexCluster != null) {
                if (mergedComplexCluster.getMissingProteins().isEmpty()) {
                    complexClusters.add(mergedComplexCluster);
                } else if (matchesToCheck.size() > 1) {
                    List<ComplexCluster> mergedClusters = checkAndMergeMatches(
                            mergedComplexCluster,
                            matchesToCheck.subList(i + 1, matchesToCheck.size()));
                    complexClusters.addAll(mergedClusters);
                }
            }
        }

        return complexClusters;
    }

    private ComplexCluster checkAndMergeMatchIntoCluster(
            ComplexCluster complexCluster,
            ComplexFinderResult.PartialMatch<IntactComplex> matchToCheck) {

        // If the new complex to match does not have any protein that is missing from the cluster, then we do not
        // consider it
        if (matchToCheck.getMatchingProteins().stream().noneMatch(protein -> complexCluster.getMissingProteins().contains(protein))) {
            return null;
        }

        // If the new complex does not have any protein in common with the proteins already in the cluster, then
        // we do not consider it, unless the cluster has not been initialised yet
        if (!complexCluster.getMatchingProteins().isEmpty() &&
                complexCluster.getMatchingProteins().stream().noneMatch(protein -> matchToCheck.getMatchingProteins().contains(protein))) {
            return null;
        }

        Set<String> matchingProteins = ImmutableSet.<String>builder()
                .addAll(complexCluster.getMatchingProteins())
                .addAll(matchToCheck.getMatchingProteins())
                .build();

        Set<String> missingProteins = complexCluster.getMissingProteins()
                .stream()
                .filter(protein -> !matchingProteins.contains(protein))
                .collect(Collectors.toSet());

        List<String> complexAcs = ImmutableList.<String>builder()
                .addAll(complexCluster.getComplexAcs())
                .add(matchToCheck.getComplexAc())
                .build();

        return new ComplexCluster(
                complexAcs,
                matchingProteins,
                missingProteins);
    }

    private void logSingleExactMatch(
            R item,
            List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches) throws IOException {

        ComplexFinderResult.ExactMatch<IntactComplex> complexMatch = exactMatches.iterator().next();
        exactMatchesReportWriter.write(
                List.of(complexMatch.getMatchType()),
                item.getComplexIds(),
                item.getProteinIds(),
                List.of(complexMatch.getComplexAc()),
                List.of(complexMatch.isPredictedComplex()));
    }

    private void logMultipleExactMatches(
            R item,
            List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches) throws IOException {

        multipleExactMatchesReportWriter.write(
                exactMatches.stream().map(ComplexFinderResult.ExactMatch::getMatchType).collect(Collectors.toList()),
                item.getComplexIds(),
                item.getProteinIds(),
                exactMatches.stream().map(ComplexFinderResult.ExactMatch::getComplexAc).collect(Collectors.toList()),
                exactMatches.stream().map(ComplexFinderResult.ExactMatch::isPredictedComplex).collect(Collectors.toList()));

        log.warn("Complexes " +
                String.join(",", item.getComplexIds()) +
                " matched exactly to multiple complexes: " +
                exactMatches
                        .stream()
                        .map(ComplexFinderResult.ExactMatch::getComplexAc)
                        .collect(Collectors.joining(",")));
    }

    private void logPartialMatches(
            R item,
            ComplexFinderResult.MatchType matchType,
            Collection<ComplexFinderResult.PartialMatch<IntactComplex>> partialMatches) throws IOException {

        for (ComplexFinderResult.PartialMatch<IntactComplex> complexMatch : partialMatches) {
            int totalNumberOfProteins = complexMatch.getMatchingProteins().size() +
                    complexMatch.getExtraProteinsInComplex().size() +
                    complexMatch.getProteinMissingInComplex().size();
            double similarity = 1d / totalNumberOfProteins * complexMatch.getMatchingProteins().size();
            partialMatchesReportWriter.write(
                    matchType,
                    item.getComplexIds(),
                    item.getProteinIds(),
                    complexMatch.getComplexAc(),
                    complexMatch.isPredictedComplex(),
                    complexMatch.getMatchingProteins().size(),
                    complexMatch.getExtraProteinsInComplex().size(),
                    complexMatch.getProteinMissingInComplex().size(),
                    similarity);
        }
    }

    private void logNoMatches(R item) throws IOException {
        noMatchesReportWriter.write(item.getComplexIds(), item.getProteinIds());
    }

    private void readComplexIdsFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();

        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        csvReader.forEach(csvLine -> {
            if (csvLine.length > 1 || (csvLine.length == 1 && !csvLine[0].isEmpty())) {
                String[] ids = csvLine[0].split(" ");
                String[] complexIds = csvLine[2].split(" ");
                for (String id: ids) {
                    matchesAlreadyProcessed.putIfAbsent(id, new HashSet<>());
                    for (String complexId : complexIds) {
                        matchesAlreadyProcessed.get(id).add(complexId);
                    }
                }
            }
        });
        csvReader.close();
    }
}

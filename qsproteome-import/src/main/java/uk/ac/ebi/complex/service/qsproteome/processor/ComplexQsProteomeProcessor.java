package uk.ac.ebi.complex.service.qsproteome.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.processor.AbstractBatchProcessor;
import uk.ac.ebi.complex.service.qsproteome.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.qsproteome.model.ComplexWithProteomeStructures;
import uk.ac.ebi.complex.service.qsproteome.reader.QsProteomeStructuresFileReader;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Log4j
@SuperBuilder
public class ComplexQsProteomeProcessor extends AbstractBatchProcessor<IntactComplex, ComplexWithProteomeStructures> {

    private static final String QS_PROTEOME_API = "https://qsproteome.org/api/lookup-entry-by-complexportal?id=";
    private static final Bucket RATE_LIMITER_BUCKET = Bucket.builder().addLimit(Bandwidth.simple(1, Duration.ofSeconds(1))).build();

    private final IntactDao intactDao;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final QsProteomeStructuresFileReader qsProteomeStructuresFileReader;

    private ProcessReportWriter noMatchesReportWriter;
    private ProcessReportWriter complexesWithStructuresReportWriter;
    private ErrorsReportWriter errorReportWriter;

    private Map<String, Collection<String>> complexesAlreadyProcessedWithMatches;
    private Set<String> complexesAlreadyProcessedWithNoMatches;

    @Override
    public ComplexWithProteomeStructures process(IntactComplex item) throws Exception {
        String complexId = item.getComplexAc();

        if (complexesAlreadyProcessedWithMatches.containsKey(complexId)) {
            return ComplexWithProteomeStructures.builder()
                    .complexId(complexId)
                    .qsProteomeIds(complexesAlreadyProcessedWithMatches.get(complexId))
                    .build();
        } else if (complexesAlreadyProcessedWithNoMatches.contains(complexId)) {
            return ComplexWithProteomeStructures.builder()
                    .complexId(complexId)
                    .qsProteomeIds(List.of())
                    .build();
        } else {
            try {
                Collection<String> structures = getQsProteomeStructuresWithRateLimit(complexId);
                if (structures.isEmpty()) {
                    noMatchesReportWriter.write(complexId, item.isPredictedComplex());
                } else {
                    complexesWithStructuresReportWriter.write(complexId, item.isPredictedComplex(), structures);
                }
                return ComplexWithProteomeStructures.builder()
                        .complexId(complexId)
                        .qsProteomeIds(structures)
                        .build();
            } catch (Exception e) {
                log.error("Error processing QSProteome structures for complex: " + complexId, e);
                errorReportWriter.write(List.of(complexId), e.getMessage());
            }
        }

        return null;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.noMatchesReportWriter.flush();
            this.complexesWithStructuresReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.noMatchesReportWriter.close();
            this.complexesWithStructuresReportWriter.close();
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

        File matchesFile = new File(reportDirectory, "complexes_with_qs_proteome_matches" + extension);
        File noMatchesFile = new File(reportDirectory, "no_matches" + extension);

        complexesAlreadyProcessedWithMatches = qsProteomeStructuresFileReader.readStructuresFromFile(matchesFile);
        complexesAlreadyProcessedWithNoMatches = qsProteomeStructuresFileReader.readComplexIdsFromFile(noMatchesFile);

        this.noMatchesReportWriter = new ProcessReportWriter(
                noMatchesFile, sep, header, ProcessReportWriter.NO_MATCHES_HEADER_LINE, true);
        this.complexesWithStructuresReportWriter = new ProcessReportWriter(
                matchesFile, sep, header, ProcessReportWriter.COMPLEXES_WITH_QS_PROTEOME_STRUCTURES, true);
        this.errorReportWriter = new ErrorsReportWriter(
                new File(reportDirectory, "process_errors" + extension), sep, header);
    }

    private Collection<String> getQsProteomeStructuresWithRateLimit(String complexId) throws IOException, InterruptedException {
        RATE_LIMITER_BUCKET.asBlocking().tryConsumeUninterruptibly(1, Duration.ofMinutes(15L));
        return getQsProteomeStructures(complexId);
    }

    private Collection<String> getQsProteomeStructures(String complexId) throws IOException, InterruptedException {
        URI urlWithParams = URI.create(QS_PROTEOME_API + complexId);
        HttpRequest request = HttpRequest.newBuilder().GET().uri(urlWithParams).build();
        HttpResponse<String> httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode response = mapper.readValue(httpResponse.body(), JsonNode.class);

        Set<String> matches = new HashSet<>();
        if (response != null) {
            if (response.isArray()) {
                ArrayNode responseArray = (ArrayNode) response;
                for (JsonNode responseItem : responseArray) {
                    if (responseItem.has("signatureMatchStatus")) {
                        if ("MATCH".equals(responseItem.get("signatureMatchStatus").asText())) {
                            if (responseItem.has("entryId")) {
                                matches.add("QS" + responseItem.get("entryId").asText());
                            }
                        }
                    }
                }
            }
        }
        return matches;
    }
}

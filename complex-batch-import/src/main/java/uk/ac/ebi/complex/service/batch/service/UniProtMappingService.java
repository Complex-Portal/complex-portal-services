package uk.ac.ebi.complex.service.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.extern.log4j.Log4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.stream.Streams;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.batch.model.UniProt;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import static uk.ac.ebi.complex.service.batch.model.UniProt.IdMapping.Poll.POLL_URL;
import static uk.ac.ebi.complex.service.batch.model.UniProt.IdMapping.Result.RESULT_URL;

@Log4j
@Component
public class UniProtMappingService {

    // Custom mappings between gene names and protein ids that cannot be resolved programmatically
    private static final Map<String, String> CUSTOM_PROTEIN_MAPPINGS = ImmutableMap.<String, String>builder()
            .put("AKAP7", "O43687")
            .put("TOR1AIP2", "Q8NFQ8")
            .put("PMF1-BGLAP", "U3KQ54")
            .put("POLR2M", "P0CAP2")
            .put("POLR1D", "P0DPB6")
            .put("ZNF689", "Q96CS4")
            .put("COMMD3-BMI1", "R4GMX3")
            .put("GNAS", "Q5JWF2")
            .build();

    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, List<UniprotProtein>> mapIds(Collection<String> ids) {
        Map<String, List<UniprotProtein>> allMappings = mapToUniprotProteins(ids, this::mapUniprotAcsHttpEntity);

        // UniProt mapping API returns too many results
        // To reduce the results, we filter our reviewed entries
        Map<String, List<UniprotProtein>> filteredMappings = new HashMap<>();
        for (String id: allMappings.keySet()) {
            List<UniprotProtein> proteins  = allMappings.get(id);
            List<UniprotProtein> reviewedProteins = filterReviewedProteins(proteins);
            if (!reviewedProteins.isEmpty()) {
                filteredMappings.put(id, reviewedProteins);
            } else {
                filteredMappings.put(id, proteins);
            }
        }
        return filteredMappings;
    }

    public Map<String, List<UniprotProtein>> mapGenes(Collection<String> geneNames) {
        Map<String, List<UniprotProtein>> allMappings = mapToUniprotProteins(geneNames, this::mapGeneNamescHttpEntity);

        // UniProt mapping API returns too many results when mapping genes
        // To reduce the results, we filter our reviewed entries and/or proteins with the primary
        // gene name matching
        Map<String, List<UniprotProtein>> filteredMappings = new HashMap<>();
        for (String geneName: allMappings.keySet()) {
            List<UniprotProtein> proteins  = allMappings.get(geneName);
            List<UniprotProtein> reviewedProteins = filterReviewedProteins(proteins);
            if (!reviewedProteins.isEmpty()) {
                if (reviewedProteins.size() == 1) {
                    filteredMappings.put(geneName, reviewedProteins);
                } else {
                    List<UniprotProtein> reviewedProteinsWithGeneName = filterProteinsWithGeneName(geneName, reviewedProteins);
                    if (!reviewedProteinsWithGeneName.isEmpty()) {
                        filteredMappings.put(geneName, reviewedProteinsWithGeneName);
                    } else {
                        filteredMappings.put(geneName, reviewedProteins);
                    }
                }
            } else {
                List<UniprotProtein> proteinsWithGeneName = filterProteinsWithGeneName(geneName, proteins);
                if (!proteinsWithGeneName.isEmpty()) {
                    filteredMappings.put(geneName, proteinsWithGeneName);
                } else {
                    filteredMappings.put(geneName, proteins);
                }
            }
        }

        for (String gene : CUSTOM_PROTEIN_MAPPINGS.keySet()) {
            if (filteredMappings.containsKey(gene) && filteredMappings.get(gene).size() > 1) {
                List<UniprotProtein> filteredProteins = filteredMappings.get(gene)
                        .stream()
                        .filter(protein -> CUSTOM_PROTEIN_MAPPINGS.get(gene).equals(protein.getProteinAc()))
                        .collect(Collectors.toList());
                if (filteredProteins.size() == 1) {
                    filteredMappings.put(gene, filteredProteins);
                }
            }
        }

        return filteredMappings;
    }

    public Map<String, List<UniprotProtein>> mapToUniprotProteins(
            Collection<String> ids,
            Function<Collection<String>, HttpEntity> entityFunction) {

        Map<String, List<UniprotProtein>> result = new HashMap<>();

        Collection<Collection<String>> collection = divideCollection(ids, 2_000);
        Streams.of(collection)
                .parallel()
                .forEach(chunk -> {
                    try {
                        String jobId = submit(entityFunction.apply(chunk));
                        var status = UniProt.IdMapping.Poll.Result.Status.RUNNING;
                        int counter = 0;
                        do {
                            Thread.sleep(1000);
                            status = pollStatus(jobId);
                            log.info("Job '" + jobId + "' with status '" + status + "'");
                            counter++;
                        } while (!status.equals(UniProt.IdMapping.Poll.Result.Status.FINISHED) || counter == 3600); // Limit to 1 hour
                        log.info("Fetching results for job '" + jobId + "'");
                        fetchResult(jobId).forEach((key, value) -> result.merge(key, value, (v1, v2) -> {
                            v1.addAll(v2);
                            return v1;
                        }));
                    } catch (IOException | InterruptedException e) {
                        log.error(e.getMessage());
                    }
                });

        Map<String, String> directs = result.entrySet()
                .stream()
                .filter(e -> e.getValue().size() == 1)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0).getProteinAc()));
        long identicalCount = directs.entrySet().stream().filter(e -> e.getValue().equals(e.getKey())).count();
        long differentCount = directs.size() - identicalCount;
        log.info("A -> A count: " + identicalCount);
        log.info("A -> B count: " + differentCount);
        log.info("A -> [null, ...] count: " + (ids.size() - directs.size()));
        return result;
    }

    private String submit(HttpEntity entity) throws IOException, InterruptedException {
        InputStream content = entity.getContent();
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> content))
                .header("Content-Type", entity.getContentType().getValue())
                .header("Accept", "application/json")
                .uri(URI.create("https://rest.uniprot.org/idmapping/run"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var result = mapper.readValue(response.body(), UniProt.IdMapping.Submit.Result.class);
        return result.getJobId();
    }

    private HttpEntity mapUniprotAcsHttpEntity(Collection<String> ids) {
        return MultipartEntityBuilder.create()
                .addPart("from", new StringBody("UniProtKB_AC-ID", ContentType.TEXT_PLAIN))
                .addPart("to", new StringBody("UniProtKB", ContentType.TEXT_PLAIN))
                .addPart("ids", new StringBody(String.join(",", ids), ContentType.TEXT_PLAIN))
                .setCharset(Charsets.UTF_8)
                .build();
    }

    private HttpEntity mapGeneNamescHttpEntity(Collection<String> ids) {
        return MultipartEntityBuilder.create()
                .addPart("from", new StringBody("Gene_Name", ContentType.TEXT_PLAIN))
                .addPart("to", new StringBody("UniProtKB", ContentType.TEXT_PLAIN))
                .addPart("ids", new StringBody(String.join(",", ids), ContentType.TEXT_PLAIN))
                .addPart("taxId", new StringBody("9606", ContentType.TEXT_PLAIN))
                .setCharset(Charsets.UTF_8)
                .build();
    }

    private UniProt.IdMapping.Poll.Result.Status pollStatus(String jobId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(POLL_URL + jobId))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var result = mapper.readValue(response.body(), UniProt.IdMapping.Poll.Result.class);
        return result.getJobStatus();
    }

    private Map<String, List<UniprotProtein>> fetchResult(String jobId) throws IOException, InterruptedException {
        Map<String, List<UniprotProtein>> result = new HashMap<>();

        Map<String, String> params = Map.of(
                "format", "tsv",
                "fields", String.join(",", List.of("accession", "reviewed", "id", "protein_name", "gene_primary", "organism_id"))
        );

        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(RESULT_URL + jobId + urlEncode(params)))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        try (CSVReader csvReader = new CSVReaderBuilder(new BufferedReader(new StringReader(response.body())))
                .withCSVParser(new CSVParserBuilder().withSeparator('\t').build())
                .build()) {
            csvReader.skip(1);
            Streams.of(csvReader.iterator()).forEach(line -> {
                String from = line[0];
                String entry = line[1];
                String reviewed = line[2];
                String name = line[3];
                String proteinName = line[4];
                String geneName = line[5];
                String organism = line[6];

                List<UniprotProtein> mappings = result.computeIfAbsent(from, k -> new ArrayList<>());
                if (!proteinName.equals("deleted")) {
                    mappings.add(new UniprotProtein(entry, name, geneName, Integer.valueOf(organism), "reviewed".equals(reviewed)));
                }
            });

        } catch (IOException ignored) {

        }

        return result;
    }

    private List<UniprotProtein> filterReviewedProteins(List<UniprotProtein> proteins) {
        return proteins.stream()
                .filter(UniprotProtein::isReviewed)
                .collect(Collectors.toList());
    }

    private List<UniprotProtein> filterProteinsWithGeneName(String geneName, List<UniprotProtein> proteins) {
        List<UniprotProtein> proteinsWithGeneName = proteins.stream()
                .filter(protein -> geneName.equals(protein.getGeneName()))
                .collect(Collectors.toList());
        if (proteinsWithGeneName.size() > 1) {
            List<UniprotProtein> proteinsWithNameMatchingGeneName = proteinsWithGeneName.stream()
                    .filter(protein -> protein.getProteinName() != null)
                    .filter(protein -> protein.getProteinName().startsWith(geneName))
                    .collect(Collectors.toList());
            if (!proteinsWithNameMatchingGeneName.isEmpty()) {
                return proteinsWithNameMatchingGeneName;
            }
        }
        return proteinsWithGeneName;
    }

    private static String urlEncode(Map<String, String> data) {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : data.entrySet()) {
            sj.add(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "=" +
                    URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        return "?" + sj;
    }

    public static <T> Collection<Collection<T>> divideCollection(Collection<T> inputCollection, int chunkSize) {
        List<Collection<T>> result = new ArrayList<>();
        List<T> currentChunk = new ArrayList<>(chunkSize);

        for (T element : inputCollection) {
            currentChunk.add(element);
            if (currentChunk.size() == chunkSize) {
                result.add(new ArrayList<>(currentChunk));
                currentChunk.clear();
            }
        }

        // Add remaining elements if any
        if (!currentChunk.isEmpty()) {
            result.add(new ArrayList<>(currentChunk));
        }

        return result;
    }

}

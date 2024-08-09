package uk.ac.ebi.complex.service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import uk.ac.ebi.complex.service.model.UniProt;

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
import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.ebi.complex.service.model.UniProt.IdMapping.Poll.POLL_URL;
import static uk.ac.ebi.complex.service.model.UniProt.IdMapping.Result.RESULT_URL;

@Log4j
@Component
public class UniProtMappingService {

    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, List<String>> mapIds(Collection<String> ids) {
        Map<String, List<String>> result = new HashMap<>();

        Collection<Collection<String>> collection = divideCollection(ids, 2_000);
        Streams.of(collection)
                .parallel()
                .forEach(chunk -> {
                    try {
                        String jobId = submit(chunk);
                        var status = UniProt.IdMapping.Poll.Result.Status.RUNNING;
                        int counter = 0;
                        do {
                            Thread.sleep(1000);
                            status = pollStatus(jobId);
                            counter++;
                        } while (!status.equals(UniProt.IdMapping.Poll.Result.Status.FINISHED) || counter == 3600); // Limit to 1 hour
                        fetchResult(jobId).forEach((key, value) -> result.merge(key, value, (v1, v2) -> {
                            v1.addAll(v2);
                            return v1;
                        }));
                    } catch (IOException | InterruptedException e) {
                        log.error(e.getMessage());
                    }
                });

        Map<String, String> directs = result.entrySet().stream().filter(e -> e.getValue().size() == 1).collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        long identicalCount = directs.entrySet().stream().filter(e -> e.getValue().equals(e.getKey())).count();
        long differentCount = directs.size() - identicalCount;
        log.info("A -> A count: " + identicalCount);
        log.info("A -> B count: " + differentCount);
        log.info("A -> [null, ...] count: " + (ids.size() - directs.size()));
        return result;
    }

    private String submit(Collection<String> ids) throws IOException, InterruptedException {

        HttpEntity entity = MultipartEntityBuilder.create()
                .addPart("from", new StringBody("UniProtKB_AC-ID", ContentType.TEXT_PLAIN))
                .addPart("to", new StringBody("UniProtKB", ContentType.TEXT_PLAIN))
                .addPart("ids", new StringBody(String.join(",", ids), ContentType.TEXT_PLAIN))
                .setCharset(Charsets.UTF_8)
                .build();
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

    private UniProt.IdMapping.Poll.Result.Status pollStatus(String jobId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(POLL_URL + jobId))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var result = mapper.readValue(response.body(), UniProt.IdMapping.Poll.Result.class);
        return result.getJobStatus();
    }

    private Map<String, List<String>> fetchResult(String jobId) throws IOException, InterruptedException {
        Map<String, List<String>> result = new HashMap<>();

        Map<String, String> params = Map.of(
                "format", "tsv",
                "fields", String.join(",", List.of("accession", "reviewed", "id", "protein_name"))
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

                List<String> mappings = result.computeIfAbsent(from, k -> new ArrayList<>());
                if (!proteinName.equals("deleted")) {
                    mappings.add(entry);
                }
            });

        } catch (IOException ignored) {

        }

        return result;
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

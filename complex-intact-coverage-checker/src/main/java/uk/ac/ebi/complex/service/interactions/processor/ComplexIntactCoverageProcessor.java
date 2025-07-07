package uk.ac.ebi.complex.service.interactions.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Interactor;
import psidev.psi.mi.jami.model.InteractorPool;
import psidev.psi.mi.jami.model.ModelledParticipant;
import psidev.psi.mi.jami.model.Protein;
import uk.ac.ebi.complex.service.interactions.model.ComplexIntactCoverage;
import uk.ac.ebi.complex.service.interactions.model.IntactProteinPairScore;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactInteractor;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

@Log4j
@RequiredArgsConstructor
public class ComplexIntactCoverageProcessor implements ItemProcessor<IntactComplex, ComplexIntactCoverage>, ItemStream {

    private static final Set<LifeCycleStatus> STATUSES_TO_CONSIDER = Set.of(
            LifeCycleStatus.READY_FOR_CHECKING,
            LifeCycleStatus.READY_FOR_RELEASE,
            LifeCycleStatus.RELEASED);

    private final String intactGraphWsUrl;
    private final HttpClient client;
    private final ObjectMapper mapper;

    private Set<String> proteinPairsWithInteractionAlreadyChecked;
    private Set<String> proteinPairsWithNoInteractionAlreadyChecked;

    @Override
    public ComplexIntactCoverage process(IntactComplex item) throws IOException, InterruptedException {
        if (item.isPredictedComplex() && STATUSES_TO_CONSIDER.contains(item.getStatus())) {
            Set<ModelledParticipant> complexParticipants = getAllComplexParticipants(item);
            Set<String> proteinAcs = getProteinAcsFromParticipants(complexParticipants);
            List<IntactProteinPairScore> intactProteinPairScores = getProteinPairScoresFromIntact(proteinAcs);
            return buildComplexIntactCoverage(item.getComplexAc(), complexParticipants, intactProteinPairScores);
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        proteinPairsWithInteractionAlreadyChecked = new HashSet<>();
        proteinPairsWithNoInteractionAlreadyChecked = new HashSet<>();
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
    }

    private Set<ModelledParticipant> getAllComplexParticipants(IntactComplex complex) {
        Set<ModelledParticipant> proteins = new HashSet<>();
        for (ModelledParticipant participant : complex.getParticipants()) {
            if (participant.getInteractor() instanceof Complex) {
                proteins.addAll(getAllComplexParticipants((IntactComplex) participant.getInteractor()));
            } else {
                proteins.add(participant);
            }
        }
        return proteins;
    }

    private Set<String> getProteinAcsFromParticipants(Set<ModelledParticipant> complexParticipants) {
        Set<String> proteinAcs = new HashSet<>();

        for (ModelledParticipant participant : complexParticipants) {
            if (participant.getInteractor() instanceof Protein) {
                proteinAcs.add(((IntactInteractor) participant.getInteractor()).getAc());
            }
            if (participant.getInteractor() instanceof InteractorPool) {
                for (Interactor subInteractor: (InteractorPool) participant.getInteractor()) {
                    if (subInteractor instanceof Protein) {
                        proteinAcs.add(((IntactInteractor) subInteractor).getAc());
                    }
                }
            }
        }
        return filterProteinsAlreadyChecked(proteinAcs);
    }

    private ComplexIntactCoverage buildComplexIntactCoverage(
            String complexAc,
            Set<ModelledParticipant> complexParticipants,
            List<IntactProteinPairScore> proteinScores) {

        List<ModelledParticipant> participantList = List.copyOf(complexParticipants);
        List<ComplexIntactCoverage.ProteinPairScore> proteinPairScores = new ArrayList<>();

        for (int i = 0; i < participantList.size(); i++) {
            for (int j = i + 1; j < participantList.size(); j++) {
                ComplexIntactCoverage.ProteinPairScore proteinPairScore = findInteractionAcWithBestScoreForParticipantsPair(
                        participantList.get(i),
                        participantList.get(j),
                        proteinScores);
                if (!filterProteinPairAlreadyChecked(proteinPairScore)) {
                    proteinPairScores.add(proteinPairScore);
                    if (proteinPairScore.getIntactScore() != null) {
                        proteinPairsWithInteractionAlreadyChecked.add(getProteinPairMergedId(proteinPairScore));
                    } else {
                        proteinPairsWithNoInteractionAlreadyChecked.add(getProteinPairMergedId(proteinPairScore));
                    }
                }
            }
        }

        return ComplexIntactCoverage.builder()
                .complexAc(complexAc)
                .proteinPairScores(proteinPairScores)
                .build();
    }

    private ComplexIntactCoverage.ProteinPairScore findInteractionAcWithBestScoreForParticipantsPair(
            ModelledParticipant participantA,
            ModelledParticipant participantB,
            List<IntactProteinPairScore> proteinPairScores) {

        Double score = null;

        Set<String> acsParticipantA = new HashSet<>();
        Set<String> acsParticipantB = new HashSet<>();

        acsParticipantA.add(((IntactInteractor) participantA.getInteractor()).getAc());
        if (participantA.getInteractor() instanceof InteractorPool) {
            for (Interactor subInteractor: (InteractorPool) participantA.getInteractor()) {
                acsParticipantA.add(((IntactInteractor) subInteractor).getAc());
            }
        }
        acsParticipantB.add(((IntactInteractor) participantB.getInteractor()).getAc());
        if (participantB.getInteractor() instanceof InteractorPool) {
            for (Interactor subInteractor: (InteractorPool) participantB.getInteractor()) {
                acsParticipantB.add(((IntactInteractor) subInteractor).getAc());
            }
        }

        for (IntactProteinPairScore proteinPairScore : proteinPairScores) {
            if (acsParticipantA.contains(proteinPairScore.getProteinA())) {
                if (acsParticipantB.contains(proteinPairScore.getProteinB())) {
                    if (score == null || proteinPairScore.getScore() > score) {
                        score = proteinPairScore.getScore();
                    }
                }
            } else if (acsParticipantB.contains(proteinPairScore.getProteinA())) {
                if (acsParticipantA.contains(proteinPairScore.getProteinB())) {
                    if (score == null || proteinPairScore.getScore() > score) {
                        score = proteinPairScore.getScore();
                    }
                }
            }
        }

        return ComplexIntactCoverage.ProteinPairScore.builder()
                .proteinA(participantA.getInteractor().getPreferredIdentifier().getId())
                .proteinB(participantB.getInteractor().getPreferredIdentifier().getId())
                .intactScore(score)
                .build();
    }

    private List<IntactProteinPairScore> getProteinPairScoresFromIntact(
            Collection<String> complexProteinIds) throws IOException, InterruptedException {

        Map<String, String> params = Map.of("proteinAcs", String.join(",", complexProteinIds));
        String intactProteinScoresEndpointUrl = intactGraphWsUrl + "interaction/protein-scores";
        return getRequest(intactProteinScoresEndpointUrl, params, new TypeReference<>() {});
    }

    private <T> T getRequest(
            String url,
            Map<String, String> params,
            TypeReference<T> typeReference) throws IOException, InterruptedException {

        URI urlWithParams = URI.create(url + encodeParams(params));
        HttpRequest request = HttpRequest.newBuilder().GET().uri(urlWithParams).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return mapper.readValue(response.body(), typeReference);
    }

    private static String encodeParams(Map<String, String> data) {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : data.entrySet()) {
            sj.add(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "=" +
                    URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        return "?" + sj;
    }

    private Set<String> filterProteinsAlreadyChecked(Set<String> proteinAcs) {
        Set<String> filteredProteinAcs = new HashSet<>();
        for (String proteinA : proteinAcs) {
            for (String proteinB : proteinAcs) {
                String mergedProteinIds;
                if (proteinA.compareTo(proteinB) <= 0) {
                    mergedProteinIds = proteinA + "_" + proteinB;
                } else {
                    mergedProteinIds = proteinB + "_" + proteinA;
                }
                if (!proteinPairsWithInteractionAlreadyChecked.contains(mergedProteinIds)) {
                    filteredProteinAcs.add(proteinA);
                    filteredProteinAcs.add(proteinB);
                }
            }
        }
        return filteredProteinAcs;
    }

    private String getProteinPairMergedId(ComplexIntactCoverage.ProteinPairScore proteinPairScore) {
        if (proteinPairScore.getProteinA().compareTo(proteinPairScore.getProteinB()) <= 0) {
            return proteinPairScore.getProteinA() + "_" + proteinPairScore.getProteinB();
        } else {
            return proteinPairScore.getProteinB() + "_" + proteinPairScore.getProteinA();
        }
    }

    private boolean filterProteinPairAlreadyChecked(ComplexIntactCoverage.ProteinPairScore proteinPairScore) {
        String mergedProteinIds = getProteinPairMergedId(proteinPairScore);
        if (proteinPairScore.getIntactScore() != null) {
            return proteinPairsWithInteractionAlreadyChecked.contains(mergedProteinIds);
        } else {
            return proteinPairsWithNoInteractionAlreadyChecked.contains(mergedProteinIds);
        }
    }
}

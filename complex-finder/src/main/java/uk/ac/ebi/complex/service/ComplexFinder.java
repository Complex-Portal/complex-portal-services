package uk.ac.ebi.complex.service;

import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.utils.CvTermUtils;
import psidev.psi.mi.jami.utils.comparator.CollectionComparator;
import psidev.psi.mi.jami.utils.comparator.participant.ModelledComparableParticipantComparator;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ComplexFinder {

    private static final float SIMILARITY_THRESHOLD = 0.5f;

    private final IntactDao intactDao;
    private final CollectionComparator<ModelledComparableParticipant> comparableParticipantsComparator;

    public ComplexFinder(IntactDao intactDao) {
        this.intactDao = intactDao;
        ModelledComparableParticipantComparator participantComparator = new ModelledComparableParticipantComparator();
        // Ignore stoichiometry for now
        participantComparator.setIgnoreStoichiometry(true);
        this.comparableParticipantsComparator = new CollectionComparator<>(participantComparator);
    }

    public ComplexFinderResult<IntactComplex> findComplexWithMatchingProteins(Collection<String> proteinAcs) {
        Collection<IntactComplex> complexes = this.intactDao.getComplexDao().getComplexesInvolvingProteins(proteinAcs);

        Collection<ModelledComparableParticipant> proteins = proteinAcs.stream()
                .map(proteinAc -> new ModelledComparableParticipant(proteinAc, 1, CvTermUtils.createProteinInteractorType()))
                .collect(Collectors.toList());

        Map<String, ComplexFinderResult.ComplexMatch<IntactComplex>> allMatches = new HashMap<>();

        for (IntactComplex complex: complexes) {
            findComplexMatches(complex, proteins, allMatches);
        }

        List<ComplexFinderResult.ComplexMatch<IntactComplex>> exactMatches = new ArrayList<>();
        List<ComplexFinderResult.ComplexMatch<IntactComplex>> partialMatches = new ArrayList<>();

        for (ComplexFinderResult.ComplexMatch<IntactComplex> complexMatch: allMatches.values()) {
            if (complexMatch != null) {
                if (complexMatch.getMatchType().equals(ComplexFinderResult.MatchType.EXACT_MATCH) ||
                        complexMatch.getMatchType().equals(ComplexFinderResult.MatchType.EXACT_MATCH_AT_PROTEIN_LEVEL)) {
                    exactMatches.add(complexMatch);
                } else {
                    partialMatches.add(complexMatch);
                }
            }
        }

        // Sort partial matches by similarity
        partialMatches.sort((a, b) -> Float.compare(b.getSimilarity(), a.getSimilarity()));

        return new ComplexFinderResult<>(proteinAcs, exactMatches, partialMatches);
    }

    private void findComplexMatches(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> proteins,
            Map<String, ComplexFinderResult.ComplexMatch<IntactComplex>> allMatches) {

        // First we check we haven't already found a match for this complex
        if (!allMatches.containsKey(complex.getComplexAc())) {
            Collection<ModelledComparableParticipant> complexProteins = complex.getComparableParticipants();

            // First we search for exact matches
            ComplexFinderResult.ComplexMatch<IntactComplex> match = findExactMatch(complex, complexProteins, proteins);
            if (match == null) {
                // If no exact matches, we look for partial matches
                match = findPartialMatch(complex, complexProteins, proteins);
            }

            if (match != null) {
                // If there is a match, we add it to the results
                allMatches.put(complex.getComplexAc(), match);

                // If the matching complex does not have extra proteins, we check if it is used
                // as a sub-complex in any other complex, and we check for matches on those super-complexes
                if (match.getMatchType() == ComplexFinderResult.MatchType.EXACT_MATCH ||
                        match.getMatchType() == ComplexFinderResult.MatchType.EXACT_MATCH_AT_PROTEIN_LEVEL ||
                        match.getMatchType() == ComplexFinderResult.MatchType.PARTIAL_MATCH_MISSING_COMPONENTS) {

                    Collection<IntactComplex> complexesWithMatchComplexAsSubComplex =
                            this.intactDao.getComplexDao().getComplexesInvolvingSubComplex(complex.getComplexAc());

                    for (IntactComplex complexB : complexesWithMatchComplexAsSubComplex) {
                        findComplexMatches(complexB, proteins, allMatches);
                    }
                }
            }
        }
    }

    private ComplexFinderResult.ComplexMatch<IntactComplex> findExactMatch(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> complexProteins,
            Collection<ModelledComparableParticipant> proteins) {

        if (this.comparableParticipantsComparator.compare(complexProteins, proteins) == 0) {
            // Exact match at protein level
            return new ComplexFinderResult.ComplexMatch<>(
                    complex.getComplexAc(),
                    getExactMatchType(complex),
                    1.0F,
                    proteins.stream().map(ModelledComparableParticipant::getInteractorId).collect(Collectors.toList()),
                    new ArrayList<>(),
                    new ArrayList<>(),
                    complex);
        }
        return null;
    }

    private ComplexFinderResult.ComplexMatch<IntactComplex> findPartialMatch(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> complexProteins,
            Collection<ModelledComparableParticipant> proteins) {

        List<String> matchingProteins = new ArrayList<>();
        List<String> extraProteins = new ArrayList<>();

        for (ModelledComparableParticipant complexComponent: complexProteins) {
            if (proteins.stream().anyMatch(protein -> protein.getInteractorId().equals(complexComponent.getInteractorId()))) {
                matchingProteins.add(complexComponent.getInteractorId());
            } else {
                extraProteins.add(complexComponent.getInteractorId());
            }
        }
        if (!matchingProteins.isEmpty()) {
            List<String> missingProteins = proteins.stream()
                    .map(ModelledComparableParticipant::getInteractorId)
                    .filter(proteinAc -> complexProteins.stream().noneMatch(component -> proteinAc.equals(component.getInteractorId())))
                    .collect(Collectors.toList());

            float similarity = calculateSimilarity(matchingProteins, complexProteins, proteins);
            if (similarity >= SIMILARITY_THRESHOLD) {
                return new ComplexFinderResult.ComplexMatch<>(
                        complex.getComplexAc(),
                        getPartialMatchType(matchingProteins, extraProteins, missingProteins),
                        similarity,
                        matchingProteins,
                        extraProteins,
                        missingProteins,
                        complex);
            }
        }
        return null;
    }

    private ComplexFinderResult.MatchType getExactMatchType(IntactComplex complex) {
        Collection<ModelledComparableParticipant> notProteinComponentIds = getNotProteinComponents(complex);
        if (notProteinComponentIds.isEmpty()) {
            return ComplexFinderResult.MatchType.EXACT_MATCH;
        } else {
            return ComplexFinderResult.MatchType.EXACT_MATCH_AT_PROTEIN_LEVEL;
        }
    }

    private ComplexFinderResult.MatchType getPartialMatchType(
            List<String> matchingProteins,
            List<String> extraProteins,
            List<String> missingProteins) {

        if (!matchingProteins.isEmpty()) {
            if (extraProteins.isEmpty()) {
                if (missingProteins.isEmpty()) {
                    // This should had been an exact match
                    throw new RuntimeException("Unexpected exact match");
                } else {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_MISSING_COMPONENTS;
                }
            } else {
                if (missingProteins.isEmpty()) {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_EXTRA_COMPONENTS;
                } else {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_MISSING_AND_EXTRA_COMPONENTS;
                }
            }
        }
        return null;
    }

    private float calculateSimilarity(List<String> matchingProteins,
                                      Collection<ModelledComparableParticipant> complexProteins,
                                      Collection<ModelledComparableParticipant> proteins) {
        int maxNumberOfProteins = Integer.max(complexProteins.size(), proteins.size());
        return (float) matchingProteins.size() / (float) maxNumberOfProteins;
    }

    private Collection<ModelledComparableParticipant> getNotProteinComponents(IntactComplex complex) {
        return complex.getAllExpandedParticipants()
                .stream()
                .filter(component -> !Protein.PROTEIN_MI.equals(component.getInteractorType().getMIIdentifier()))
                .collect(Collectors.toList());
    }
}

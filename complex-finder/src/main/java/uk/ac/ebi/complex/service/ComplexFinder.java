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

    private static final int NUMBER_OF_DIFFERENT_PROTEINS_FOR_PARTIAL_MATCHES = 1;

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

        Map<String, ComplexFinderResult.ExactMatch<IntactComplex>> exactMatchesMap = new HashMap<>();
        Map<String, ComplexFinderResult.PartialMatch<IntactComplex>> partialMatchesMap = new HashMap<>();

        for (IntactComplex complex: complexes) {
            findComplexMatches(complex, proteins, exactMatchesMap, partialMatchesMap);
        }

        List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches = new ArrayList<>(exactMatchesMap.values());
        List<ComplexFinderResult.PartialMatch<IntactComplex>> partialMatches = new ArrayList<>(partialMatchesMap.values());

        return new ComplexFinderResult<>(proteinAcs, exactMatches, partialMatches);
    }

    private void findComplexMatches(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> proteins,
            Map<String, ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches,
            Map<String, ComplexFinderResult.PartialMatch<IntactComplex>> partialMatches) {

        // First we check we haven't already found a match for this complex
        if (!exactMatches.containsKey(complex.getComplexAc()) && !partialMatches.containsKey(complex.getComplexAc())) {
            Collection<ModelledComparableParticipant> complexProteins = complex.getComparableParticipants();

            // First we search for exact matches
            ComplexFinderResult.ExactMatch<IntactComplex> exactMatch = findExactMatch(complex, complexProteins, proteins);
            ComplexFinderResult.PartialMatch<IntactComplex> partialMatch = null;
            if (exactMatch == null) {
                // If no exact matches, we look for partial matches
                partialMatch = findPartialMatch(complex, complexProteins, proteins);
            }

            if (exactMatch != null) {
                // If there is an exact match, we add it to the results
                exactMatches.put(complex.getComplexAc(), exactMatch);

                // We also check if it is used as a sub-complex in any other complex,
                // and we check for matches on those super-complexes
                Collection<IntactComplex> complexesWithMatchComplexAsSubComplex =
                        this.intactDao.getComplexDao().getComplexesInvolvingSubComplex(complex.getComplexAc());

                for (IntactComplex complexB : complexesWithMatchComplexAsSubComplex) {
                    findComplexMatches(complexB, proteins, exactMatches, partialMatches);
                }
            }

            if (partialMatch != null) {
                // If there is a partial match, we add it to the results
                partialMatches.put(complex.getComplexAc(), partialMatch);

                // If the matching complex does not have extra proteins, we check if it is used
                // as a sub-complex in any other complex, and we check for matches on those super-complexes
                if (partialMatch.getMatchType().equals(ComplexFinderResult.MatchType.PARTIAL_MATCH_MISSING_COMPONENTS)) {
                    Collection<IntactComplex> complexesWithMatchComplexAsSubComplex =
                            this.intactDao.getComplexDao().getComplexesInvolvingSubComplex(complex.getComplexAc());

                    for (IntactComplex complexB : complexesWithMatchComplexAsSubComplex) {
                        findComplexMatches(complexB, proteins, exactMatches, partialMatches);
                    }
                }
            }
        }
    }

    private ComplexFinderResult.ExactMatch<IntactComplex> findExactMatch(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> complexProteins,
            Collection<ModelledComparableParticipant> proteins) {

        if (this.comparableParticipantsComparator.compare(complexProteins, proteins) == 0) {
            // Exact match at protein level
            return new ComplexFinderResult.ExactMatch<>(
                    complex.getComplexAc(),
                    getExactMatchType(complex),
                    complex);
        }
        return null;
    }

    private ComplexFinderResult.PartialMatch<IntactComplex> findPartialMatch(
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

            if ((extraProteins.size() + missingProteins.size()) <= NUMBER_OF_DIFFERENT_PROTEINS_FOR_PARTIAL_MATCHES) {
                return new ComplexFinderResult.PartialMatch<>(
                        complex.getComplexAc(),
                        getPartialMatchType(matchingProteins, extraProteins, missingProteins),
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

    private Collection<ModelledComparableParticipant> getNotProteinComponents(IntactComplex complex) {
        return complex.getAllExpandedParticipants()
                .stream()
                .filter(component -> !Protein.PROTEIN_MI.equals(component.getInteractorType().getMIIdentifier()))
                .collect(Collectors.toList());
    }
}

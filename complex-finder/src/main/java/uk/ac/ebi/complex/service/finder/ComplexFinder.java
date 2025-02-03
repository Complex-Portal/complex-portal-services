package uk.ac.ebi.complex.service.finder;

import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.model.impl.DefaultCvTerm;
import psidev.psi.mi.jami.model.impl.DefaultXref;
import psidev.psi.mi.jami.utils.CvTermUtils;
import psidev.psi.mi.jami.utils.comparator.CollectionComparator;
import psidev.psi.mi.jami.utils.comparator.participant.ModelledComparableParticipantComparator;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ComplexFinder {

    private final IntactDao intactDao;
    private final CollectionComparator<ModelledComparableParticipant> comparableParticipantsComparator;

    public ComplexFinder(IntactDao intactDao) {
        this.intactDao = intactDao;
        ModelledComparableParticipantComparator participantComparator = new ModelledComparableParticipantComparator();
        // Ignore stoichiometry for now
        participantComparator.setIgnoreStoichiometry(true);
        this.comparableParticipantsComparator = new CollectionComparator<>(participantComparator);
    }

    public ComplexFinderResult<IntactComplex> findComplexWithMatchingProteins(
            Collection<String> proteinIds,
            ComplexFinderOptions complexFinderOptions) {

        Collection<IntactComplex> allComplexes = getComplexesInvolvingProteins(proteinIds);

        Collection<IntactComplex> complexes;
        if (complexFinderOptions.isCheckPredictedComplexes()) {
            complexes = allComplexes;
        } else {
            complexes = allComplexes.stream()
                    .filter(complex -> !complex.isPredictedComplex()) // Filter out predicted complexes
                    .collect(Collectors.toList());
        }

        Collection<ModelledComparableParticipant> proteins = proteinIds.stream()
                .map(proteinId -> new ModelledComparableParticipant(
                        proteinId,
                        List.of(new DefaultXref(
                                new DefaultCvTerm(Xref.UNIPROTKB, Xref.UNIPROTKB_MI),
                                proteinId,
                                new DefaultCvTerm(Xref.IDENTITY, Xref.IDENTITY_MI))),
                        1,
                        CvTermUtils.createProteinInteractorType()))
                .collect(Collectors.toList());

        Map<String, IntactProtein> proteinCacheMap = new HashMap<>();
        Map<String, ComplexFinderResult.ExactMatch<IntactComplex>> exactMatchesMap = new HashMap<>();
        Map<String, ComplexFinderResult.PartialMatch<IntactComplex>> partialMatchesMap = new HashMap<>();

        for (IntactComplex complex : complexes) {
            findComplexMatches(complex.getComplexAc(), proteins, proteinCacheMap, exactMatchesMap, partialMatchesMap, complexFinderOptions);
        }

        List<ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches = new ArrayList<>(exactMatchesMap.values());
        List<ComplexFinderResult.PartialMatch<IntactComplex>> partialMatches = complexFinderOptions.isCheckPartialMatches()
                ? new ArrayList<>(partialMatchesMap.values())
                : List.of();

        return new ComplexFinderResult<>(proteinIds, exactMatches, partialMatches);
    }

    private void findComplexMatches(
            String complexAc,
            Collection<ModelledComparableParticipant> proteins,
            Map<String, IntactProtein> proteinCacheMap,
            Map<String, ComplexFinderResult.ExactMatch<IntactComplex>> exactMatches,
            Map<String, ComplexFinderResult.PartialMatch<IntactComplex>> partialMatches,
            ComplexFinderOptions complexFinderOptions) {

        // First we check we haven't already found a match for this complex
        if (!exactMatches.containsKey(complexAc) && !partialMatches.containsKey(complexAc)) {
            IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(complexAc);

            // By default, we only compare against complexes released or ready for release
            if (!complexFinderOptions.isCheckAnyStatusForExactMatches() &&
                    !LifeCycleStatus.RELEASED.equals(complex.getStatus()) &&
                    !LifeCycleStatus.READY_FOR_RELEASE.equals(complex.getStatus())) {
                return;
            }

            Collection<ModelledComparableParticipant> curatedComplexProteins = getProteinComponents(complex, proteinCacheMap);

            // First we search for exact matches
            ComplexFinderResult.ExactMatch<IntactComplex> exactMatch = findExactMatch(complex, curatedComplexProteins, proteins);

            if (exactMatch != null) {
                // If there is an exact match, we add it to the results
                exactMatches.put(complex.getComplexAc(), exactMatch);

                // We also check if it is used as a sub-complex in any other complex,
                // and we check for matches on those super-complexes
                Collection<IntactComplex> complexesWithMatchComplexAsSubComplex =
                        this.intactDao.getComplexDao().getComplexesInvolvingSubComplex(complex.getComplexAc());

                for (IntactComplex complexB : complexesWithMatchComplexAsSubComplex) {
                    findComplexMatches(complexB.getComplexAc(), proteins, proteinCacheMap, exactMatches, partialMatches, complexFinderOptions);
                }
            } else {
                // If no exact matches, we look for partial matches

                // For partial matches, we only consider complexes released or ready for release, and only curated complexes
                if (!complex.isPredictedComplex() &&
                        (LifeCycleStatus.RELEASED.equals(complex.getStatus()) || LifeCycleStatus.READY_FOR_RELEASE.equals(complex.getStatus()))) {

                    ComplexFinderResult.PartialMatch<IntactComplex> partialMatch = findPartialMatch(complex, curatedComplexProteins, proteins);

                    if (partialMatch != null) {
                        // If there is a partial match, we add it to the results
                        partialMatches.put(complex.getComplexAc(), partialMatch);

                        // If the matching complex does not have extra proteins, we check if it is used
                        // as a sub-complex in any other complex, and we check for matches on those super-complexes
                        if (partialMatch.getMatchType().equals(ComplexFinderResult.MatchType.PARTIAL_MATCH_PROTEINS_MISSING_IN_COMPLEX)) {
                            Collection<IntactComplex> complexesWithMatchComplexAsSubComplex =
                                    this.intactDao.getComplexDao().getComplexesInvolvingSubComplex(complex.getComplexAc());

                            for (IntactComplex complexB : complexesWithMatchComplexAsSubComplex) {
                                findComplexMatches(complexB.getComplexAc(), proteins, proteinCacheMap, exactMatches, partialMatches, complexFinderOptions);
                            }
                        }
                    }
                }
            }
        }
    }

    private ComplexFinderResult.ExactMatch<IntactComplex> findExactMatch(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> curatedComplexProteins,
            Collection<ModelledComparableParticipant> proteins) {

        if (this.comparableParticipantsComparator.compare(curatedComplexProteins, proteins) == 0) {
            // Exact match at protein level
            return new ComplexFinderResult.ExactMatch<>(
                    complex.getComplexAc(),
                    complex.isPredictedComplex(),
                    getExactMatchType(complex),
                    complex);
        }
        return null;
    }

    private ComplexFinderResult.PartialMatch<IntactComplex> findPartialMatch(
            IntactComplex complex,
            Collection<ModelledComparableParticipant> curatedComplexProteins,
            Collection<ModelledComparableParticipant> proteins) {

        List<String> matchingProteins = new ArrayList<>();
        List<String> proteinMissingInComplex = new ArrayList<>();

        for (ModelledComparableParticipant protein: proteins) {
            if (curatedComplexProteins.stream().anyMatch(complexProtein -> complexProtein.getInteractorId().equals(protein.getInteractorId()))) {
                matchingProteins.add(protein.getInteractorId());
            } else {
                proteinMissingInComplex.add(protein.getInteractorId());
            }
        }
        if (!matchingProteins.isEmpty()) {
            List<String> extraProteinsInComplex = curatedComplexProteins.stream()
                    .map(ModelledComparableParticipant::getInteractorId)
                    .filter(proteinId -> proteins.stream().noneMatch(protein -> proteinId.equals(protein.getInteractorId())))
                    .collect(Collectors.toList());

            return new ComplexFinderResult.PartialMatch<>(
                    complex.getComplexAc(),
                    complex.isPredictedComplex(),
                    getPartialMatchType(matchingProteins, proteinMissingInComplex, extraProteinsInComplex),
                    matchingProteins,
                    extraProteinsInComplex,
                    proteinMissingInComplex,
                    complex);
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
            List<String> proteinMissingInComplex,
            List<String> extraProteinsInComplex) {

        if (!matchingProteins.isEmpty()) {
            if (proteinMissingInComplex.isEmpty()) {
                if (extraProteinsInComplex.isEmpty()) {
                    // This should had been an exact match
                    throw new RuntimeException("Unexpected exact match");
                } else {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_SUBSET_OF_COMPLEX;
                }
            } else {
                if (extraProteinsInComplex.isEmpty()) {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_PROTEINS_MISSING_IN_COMPLEX;
                } else {
                    return ComplexFinderResult.MatchType.PARTIAL_MATCH_OTHER;
                }
            }
        }
        return null;
    }

    private Collection<ModelledComparableParticipant> getProteinComponents(
            IntactComplex complex,
            Map<String, IntactProtein> proteinCacheMap) {

        return complex.getComparableParticipants(
                true,
                proteinAc -> {
                    if (!proteinCacheMap.containsKey(proteinAc)) {
                        IntactProtein protein = intactDao.getProteinDao().getByAc(proteinAc);
                        proteinCacheMap.put(proteinAc, protein);
                    }
                    return proteinCacheMap.get(proteinAc);
                }
        );
    }

    private Collection<ModelledComparableParticipant> getNotProteinComponents(IntactComplex complex) {
        return complex.getAllExpandedParticipants()
                .stream()
                .filter(component -> !Protein.PROTEIN_MI.equals(component.getInteractorType().getMIIdentifier()))
                .collect(Collectors.toList());
    }

    private Collection<IntactComplex> getComplexesInvolvingProteins(Collection<String> proteinIds) {
        Collection<IntactProtein> proteins = this.intactDao.getProteinDao().getByCanonicalIds(Xref.UNIPROTKB_MI, proteinIds);
        Collection<String> proteinAcs = proteins.stream().map(IntactProtein::getAc).collect(Collectors.toList());
        return this.intactDao.getComplexDao().getComplexesInvolvingProteinsWithEbiAcs(proteinAcs);
    }
}

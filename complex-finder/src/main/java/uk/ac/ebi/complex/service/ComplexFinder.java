package uk.ac.ebi.complex.service;

import lombok.AllArgsConstructor;
import psidev.psi.mi.jami.model.Interactor;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.CvTermUtils;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ComplexFinder {

    private final IntactDao intactDao;

    public ComplexFinderResult<IntactComplex> findComplexWithMatchingProteins(Collection<String> proteinAcs) {
        Collection<IntactComplex> complexes = this.intactDao.getComplexDao()
                .getComplexesInvolvingInteractors(Xref.UNIPROTKB_MI, proteinAcs);

        List<ComplexFinderResult.ComplexMatch<IntactComplex>> exactMatches = new ArrayList<>();
        List<ComplexFinderResult.ComplexMatch<IntactComplex>> partialMatches = new ArrayList<>();

        for (IntactComplex complex : complexes) {
            ComplexFinderResult.ComplexMatch<IntactComplex> complexMatch = findComplexMatch(complex, proteinAcs);
            if (complexMatch != null) {
                if (complexMatch.getMatchType().equals(ComplexFinderResult.MatchType.EXACT_MATCH) ||
                        complexMatch.getMatchType().equals(ComplexFinderResult.MatchType.EXACT_MATCH_AT_PROTEIN_LEVEL)) {
                    exactMatches.add(complexMatch);
                } else {
                    partialMatches.add(complexMatch);
                }
            }
        }

        return new ComplexFinderResult<>(proteinAcs, exactMatches, partialMatches);
    }

    private ComplexFinderResult.ComplexMatch<IntactComplex> findComplexMatch(IntactComplex complex, Collection<String> proteinAcs) {
        List<ComplexFinderResult.ComplexComponent> matchingProteins = new ArrayList<>();
        List<ComplexFinderResult.ComplexComponent> extraProteins = new ArrayList<>();

        Collection<ComplexFinderResult.ComplexComponent> complexProteins = getComplexProteins(complex);
        for (ComplexFinderResult.ComplexComponent complexComponent: complexProteins) {
            if (proteinAcs.stream().anyMatch(proteinAc ->
                    proteinAc.equals(complexComponent.getPrimaryId()) || complexComponent.getOtherIds().stream().anyMatch(proteinAc::equals))) {
                matchingProteins.add(complexComponent);
            } else {
                extraProteins.add(complexComponent);
            }
        }
        if (!matchingProteins.isEmpty()) {
            List<String> missingProteins = proteinAcs.stream()
                    .filter(proteinAc ->
                            complexProteins.stream().noneMatch(component ->
                                    proteinAc.equals(component.getPrimaryId()) || component.getOtherIds().stream().anyMatch(proteinAc::equals)))
                    .collect(Collectors.toList());

            return new ComplexFinderResult.ComplexMatch<>(
                    complex.getComplexAc(),
                    getMatchType(complex, matchingProteins, extraProteins, missingProteins),
                    matchingProteins,
                    extraProteins,
                    missingProteins,
                    complex);
        }
        return null;
    }

    private ComplexFinderResult.MatchType getMatchType(
            IntactComplex complex,
            List<ComplexFinderResult.ComplexComponent> matchingProteins,
            List<ComplexFinderResult.ComplexComponent> extraProteins,
            List<String> missingProteins) {

        if (!matchingProteins.isEmpty()) {
            if (extraProteins.isEmpty()) {
                if (missingProteins.isEmpty()) {
                    Collection<ComplexFinderResult.ComplexComponent> otherComplexComponents = getComplexOtherComponents(complex);
                    if (otherComplexComponents.isEmpty()) {
                        return ComplexFinderResult.MatchType.EXACT_MATCH;
                    } else {
                        return ComplexFinderResult.MatchType.EXACT_MATCH_AT_PROTEIN_LEVEL;
                    }
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

    private Collection<ComplexFinderResult.ComplexComponent> getComplexProteins(IntactComplex complex) {
        return getComplexOtherComponents(complex, this::isInteractorProtein);
    }

    private Collection<ComplexFinderResult.ComplexComponent> getComplexOtherComponents(IntactComplex complex) {
        return getComplexOtherComponents(complex, (interactor) -> !isInteractorProtein(interactor));
    }

    private Collection<ComplexFinderResult.ComplexComponent> getComplexOtherComponents(IntactComplex complex, Predicate<Interactor> interactorFilter) {
        return complex.getParticipants()
                .stream()
                .filter(component -> interactorFilter.test(component.getInteractor()))
                .collect(Collectors.toMap(
                        component -> component.getInteractor().getPreferredIdentifier().getId(),
                        component -> new ComplexFinderResult.ComplexComponent(
                                component.getInteractor().getPreferredIdentifier().getId(),
                                component.getInteractor().getIdentifiers().stream().map(Xref::getId).collect(Collectors.toSet())),
                        (a, b) -> a))
                .values();
    }

    private boolean isInteractorProtein(Interactor interactor) {
        return CvTermUtils.isCvTerm(interactor.getInteractorType(), Protein.PROTEIN_MI, Protein.PROTEIN) ||
                CvTermUtils.isCvTerm(interactor.getInteractorType(), Protein.PEPTIDE_MI, Protein.PEPTIDE);
    }
}

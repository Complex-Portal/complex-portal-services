package uk.ac.ebi.complex.service.finder;

import lombok.AllArgsConstructor;
import psidev.psi.mi.jami.model.Interactor;
import psidev.psi.mi.jami.model.ModelledParticipant;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactInteractorPool;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ComplexOrthologFinder {

    protected final static String ORTHOLOGY_MI = "MI:2426";
    private final IntactDao intactDao;

    public Collection<IntactComplex> findComplexOrthologs(String complexId, Integer taxId) {
        final IntactComplex complex;
        if (complexId.startsWith("CPX-")) {
            complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(complexId);
        } else {
            complex = intactDao.getComplexDao().getByAc(complexId);
        }
        Collection<String> proteinOrthologIds = getOrthologIds(complex);

        Collection<IntactComplex> complexes = findAllComplexesWithSameOrthologs(taxId, proteinOrthologIds);
        return complexes.stream()
                .filter(complexMatch -> !complexMatch.getComplexAc().equals(complex.getComplexAc()))
                .collect(Collectors.toList());
    }

    private Collection<String> getOrthologIds(IntactComplex complex) {
        Map<String, Collection<String>> proteinToOrthologsMap = getOrthologIdsByProtein(complex);

        Set<String> orthologIds = new HashSet<>();
        for (String proteinId: proteinToOrthologsMap.keySet()) {
            Collection<String> proteinOrthologIds = proteinToOrthologsMap.get(proteinId);
            if (proteinOrthologIds.isEmpty()) {
                return Set.of();
            } else {
                orthologIds.add(proteinOrthologIds.iterator().next());
            }
        }
        return orthologIds;
    }

    private Map<String, Collection<String>> getOrthologIdsByProtein(IntactComplex complex) {
        Map<String, Collection<String>> proteinToOrthologsMap = new HashMap<>();
        for (ModelledParticipant participant: complex.getParticipants()) {
            Interactor interactor = participant.getInteractor();
            if (interactor instanceof IntactProtein) {
                IntactProtein protein = (IntactProtein) interactor;
                Collection<Xref> orthologXrefs = XrefUtils.collectAllXrefsHavingQualifier(protein.getXrefs(), ORTHOLOGY_MI, null);
                proteinToOrthologsMap.put(protein.getAc(), orthologXrefs.stream().map(Xref::getId).collect(Collectors.toList()));
            } else if (interactor instanceof IntactInteractorPool) {
                IntactInteractorPool pool = (IntactInteractorPool) interactor;
                for (Interactor subInteractor: pool) {
                    if (subInteractor instanceof IntactProtein) {
                        IntactProtein protein = (IntactProtein) subInteractor;
                        Collection<Xref> orthologXrefs = XrefUtils.collectAllXrefsHavingQualifier(protein.getXrefs(), ORTHOLOGY_MI, null);
                        proteinToOrthologsMap.put(protein.getAc(), orthologXrefs.stream().map(Xref::getId).collect(Collectors.toList()));
                    }
                }
            } else if (interactor instanceof IntactComplex) {
                proteinToOrthologsMap.putAll(getOrthologIdsByProtein((IntactComplex) interactor));
            }
        }
        return proteinToOrthologsMap;
    }

    private Collection<IntactComplex> findAllComplexesWithSameOrthologs(Integer taxId, Collection<String> orthologIds) {
        Collection<IntactComplex> complexes = findComplexesWithSameOrthologs(orthologIds);
        return findAllComplexesWithAllOrthologsMatching(taxId, orthologIds, complexes);
    }

    private Collection<IntactComplex> findAllComplexesWithAllOrthologsMatching(
            Integer taxId,
            Collection<String> orthologIds,
            Collection<IntactComplex> complexesPartiallyMatching) {

        List<IntactComplex> complexesWithAllMatchingOrthologs = new ArrayList<>();
        List<String> complexesAcsToCheckAsSubcomplexes = new ArrayList<>();

        for (IntactComplex complex : complexesPartiallyMatching) {
            if (taxId == null || taxId.equals(complex.getOrganism().getTaxId())) {
                Collection<String> complexOrthologIds = getOrthologIds(complex);
                if (!complexOrthologIds.isEmpty() && orthologIds.containsAll(complexOrthologIds)) {
                    if (complexOrthologIds.containsAll(orthologIds)) {
                        complexesWithAllMatchingOrthologs.add(complex);
                    }
                    complexesAcsToCheckAsSubcomplexes.add(complex.getComplexAc());
                }
            }
        }

        if (!complexesAcsToCheckAsSubcomplexes.isEmpty()) {
            Collection<IntactComplex> complexesWithSubcomplex = findComplexesWithSubComplexes(complexesAcsToCheckAsSubcomplexes);
            if (!complexesWithSubcomplex.isEmpty()) {
                complexesWithAllMatchingOrthologs.addAll(findAllComplexesWithAllOrthologsMatching(
                        taxId,
                        orthologIds,
                        complexesWithSubcomplex));
            }
        }

        return complexesWithAllMatchingOrthologs;
    }

    private Collection<IntactComplex> findComplexesWithSameOrthologs(Collection<String> orthologIds) {
        Query query = intactDao.getEntityManager().createQuery("select distinct complex " +
                "from IntactComplex complex " +
                "join complex.participants as participant " +
                "join participant.interactor as interactor " +
                "join interactor.dbXrefs as xref " +
                "join xref.qualifier as qualifier " +
                "where qualifier.identifier = :orthologQualifierId " +
                "and xref.id in (:orthologIds) " +
                "and not exists (" +
                "  select distinct complex2.ac " +
                "  from IntactComplex complex2 " +
                "  join complex2.participants as participant2 " +
                "  join participant2.interactor as interactor2 " +
                "  join interactor2.dbXrefs as xref2 " +
                "  join xref2.qualifier as qualifier2 " +
                "  where complex.ac = complex2.ac " +
                "  and qualifier2.identifier = :orthologQualifierId " +
                "  and xref2.id not in (:orthologIds) " +
                ")");
        query.setParameter("orthologQualifierId", ORTHOLOGY_MI);
        query.setParameter("orthologIds", orthologIds);
        return query.getResultList();
    }

    private Collection<IntactComplex> findComplexesWithSubComplexes(Collection<String> subcomplexesAcs) {
        Query query = intactDao.getEntityManager().createQuery("select distinct complex " +
                "from IntactComplex complex " +
                "join complex.participants as participant " +
                "join participant.interactor as interactor " +
                "where interactor.ac in (:subcomplexesAcs)");
        query.setParameter("subcomplexesAcs", subcomplexesAcs);
        return query.getResultList();
    }
}

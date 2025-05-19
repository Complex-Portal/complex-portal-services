package uk.ac.ebi.complex.service.finder;

import lombok.AllArgsConstructor;
import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;

import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ComplexOrthologFinder {

    private final static String ORTHOLOGY_MI = "MI:2426";
    private final IntactDao intactDao;

    public Collection<IntactComplex> findComplexOrthologs(String complexId) {
        return findComplexOrthologs(complexId, null);
    }

    public Collection<IntactComplex> findComplexOrthologs(String complexId, Integer taxId) {
        Map<String, IntactProtein> proteinCacheMap = new HashMap<>();

        IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(complexId);
        Collection<String> proteinOrthologIds = getOrthologIds(complex, proteinCacheMap);

        Collection<IntactComplex> complexes = findAllComplexesWithSameOrthologs(taxId, proteinOrthologIds, proteinCacheMap);
        return complexes.stream()
                .filter(complexMatch -> !complexMatch.getComplexAc().equals(complex.getComplexAc()))
                .collect(Collectors.toList());
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

    private Collection<IntactProtein> getProteins(IntactComplex complex, Map<String, IntactProtein> proteinCacheMap) {
        Collection<ModelledComparableParticipant> participants = getProteinComponents(complex, proteinCacheMap);
        Collection<String> proteinIds = participants.stream()
                .map(ModelledComparableParticipant::getInteractorId)
                .collect(Collectors.toList());
        return this.intactDao.getProteinDao().getByCanonicalIds(Xref.UNIPROTKB_MI, proteinIds);
    }

    private Collection<String> getOrthologIds(IntactComplex complex, Map<String, IntactProtein> proteinCacheMap) {
        Collection<IntactProtein> complexProteins = getProteins(complex, proteinCacheMap);
        Collection<String> proteinOrthologIds = new ArrayList<>();
        for (IntactProtein protein : complexProteins) {
            Collection<Xref> orthologXrefs = XrefUtils.collectAllXrefsHavingQualifier(protein.getXrefs(), ORTHOLOGY_MI, null);
            if (orthologXrefs.isEmpty()) {
                return List.of();
            }
            Xref orthologXref = orthologXrefs.iterator().next();
            proteinOrthologIds.add(orthologXref.getId());
        }
        return proteinOrthologIds;
    }

    private Collection<IntactComplex> findAllComplexesWithSameOrthologs(
            Integer taxId,
            Collection<String> orthologIds,
            Map<String, IntactProtein> proteinCacheMap) {

        Collection<IntactComplex> complexes = findComplexesWithSameOrthologs(orthologIds);
        return findAllComplexesWithAllOrthologsMatching(
                taxId,
                orthologIds,
                complexes,
                proteinCacheMap);
    }

    private Collection<IntactComplex> findAllComplexesWithAllOrthologsMatching(
            Integer taxId,
            Collection<String> orthologIds,
            Collection<IntactComplex> complexesPartiallyMatching,
            Map<String, IntactProtein> proteinCacheMap) {

        List<IntactComplex> complexesWithAllMatchingOrthologs = new ArrayList<>();
        List<String> complexesAcsToCheckAsSubcomplexes = new ArrayList<>();

        for (IntactComplex complex : complexesPartiallyMatching) {
            if (taxId == null || taxId.equals(complex.getOrganism().getTaxId())) {
                Collection<String> complexOrthologIds = getOrthologIds(complex, proteinCacheMap);
                if (!complexOrthologIds.isEmpty() && orthologIds.containsAll(complexOrthologIds)) {
                    if (complexOrthologIds.size() == orthologIds.size()) {
                        complexesWithAllMatchingOrthologs.add(complex);
                    }
                    complexesAcsToCheckAsSubcomplexes.add(complex.getComplexAc());
                }
            }
        }

        Collection<IntactComplex> complexesWithSubcomplex = findComplexesWithSubComplexes(complexesAcsToCheckAsSubcomplexes);
        if (!complexesWithSubcomplex.isEmpty()) {
            complexesWithAllMatchingOrthologs.addAll(findAllComplexesWithAllOrthologsMatching(
                    taxId,
                    orthologIds,
                    complexesWithSubcomplex,
                    proteinCacheMap));
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
                "and xref.id in (:orthologIds)");
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

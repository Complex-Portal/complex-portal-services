package uk.ac.ebi.complex.service.finder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import psidev.psi.mi.jami.model.CvTerm;
import psidev.psi.mi.jami.model.InteractorPool;
import psidev.psi.mi.jami.model.Organism;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.model.impl.DefaultCvTerm;
import psidev.psi.mi.jami.model.impl.DefaultOrganism;
import psidev.psi.mi.jami.model.impl.DefaultXref;
import uk.ac.ebi.intact.jami.dao.ComplexDao;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactInteractorPool;
import uk.ac.ebi.intact.jami.model.extension.IntactModelledParticipant;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ComplexOrthologFinderTest {

    @Mock
    private Query query;
    @Mock
    private EntityManager entityManager;
    @Mock
    private ComplexDao complexDao;
    @Mock
    private IntactDao intactDao;

    private ComplexOrthologFinder complexOrthologFinder;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(query).when(entityManager).createQuery(Mockito.anyString());
        Mockito.doReturn(entityManager).when(intactDao).getEntityManager();
        Mockito.doReturn(complexDao).when(intactDao).getComplexDao();

        complexOrthologFinder = new ComplexOrthologFinder(intactDao);
    }

    @Test
    public void findComplexOrthologOneMatchFound() {
        String ortholog1 = "ortholog-1";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));

        String orthologComplexId = "CPX-2";
        IntactComplex orthologComplex = new IntactComplex("test");
        orthologComplex.assignComplexAc(orthologComplexId);
        orthologComplex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog1))));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex), List.of()).when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertEquals(1, orthologs.size());
        Assert.assertEquals(orthologComplex, orthologs.iterator().next());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologByEbiAcOneMatchFound() {
        String ortholog1 = "ortholog-1";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.setAc("EBI-10");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));

        String orthologComplexId = "CPX-2";
        IntactComplex orthologComplex = new IntactComplex("test");
        orthologComplex.assignComplexAc(orthologComplexId);
        orthologComplex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog1))));

        Mockito.doReturn(complex).when(complexDao).getByAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex), List.of()).when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complex.getAc(), null);

        Assert.assertEquals(1, orthologs.size());
        Assert.assertEquals(orthologComplex, orthologs.iterator().next());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getByAc(complex.getAc());
    }

    @Test
    public void findComplexOrthologNoFullMatchFound() {
        String ortholog1 = "ortholog-1";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));
        complex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref("ortholog-2"))));

        String orthologComplexId = "CPX-2";
        IntactComplex orthologComplex = new IntactComplex("test");
        orthologComplex.assignComplexAc(orthologComplexId);
        orthologComplex.addParticipant(
                buildComplexParticipant("EBI-3", List.of(buildOrthologXref(ortholog1))));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex), List.of()).when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertTrue(orthologs.isEmpty());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologMultipleMatchesFound() {
        String ortholog1 = "ortholog-1";
        String ortholog2 = "ortholog-2";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));
        complex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog2))));

        String orthologComplexId1 = "CPX-2";
        IntactComplex orthologComplex1 = new IntactComplex("test");
        orthologComplex1.assignComplexAc(orthologComplexId1);
        orthologComplex1.addParticipant(
                buildComplexParticipant("EBI-3", List.of(buildOrthologXref(ortholog1))));
        orthologComplex1.addParticipant(
                buildComplexParticipant("EBI-4", List.of(buildOrthologXref(ortholog2))));

        String orthologComplexId2 = "CPX-3";
        IntactComplex orthologComplex2 = new IntactComplex("test");
        orthologComplex2.assignComplexAc(orthologComplexId2);
        orthologComplex2.addParticipant(
                buildComplexParticipant("EBI-5", List.of(buildOrthologXref(ortholog1))));
        orthologComplex2.addParticipant(
                buildComplexParticipant("EBI-6", List.of(buildOrthologXref(ortholog2))));

        String noOrthologComplexId = "CPX-4";
        IntactComplex noOrthologComplex = new IntactComplex("test");
        noOrthologComplex.assignComplexAc(noOrthologComplexId);
        noOrthologComplex.addParticipant(
                buildComplexParticipant("EBI-7", List.of(buildOrthologXref(ortholog1))));
        noOrthologComplex.addParticipant(
                buildComplexParticipant("EBI-8", List.of(buildOrthologXref("other-ortholog"))));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex1, orthologComplex2, noOrthologComplex), List.of())
                .when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertEquals(2, orthologs.size());
        Assert.assertEquals(
                Set.of(orthologComplexId1, orthologComplexId2),
                orthologs.stream().map(IntactComplex::getComplexAc).collect(Collectors.toSet()));

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologWithSubComplexes() {
        String ortholog1 = "ortholog-1";
        String ortholog2 = "ortholog-2";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));
        complex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog2))));

        String subComplexId = "CPX-2";
        IntactComplex subComplex = new IntactComplex("test");
        subComplex.assignComplexAc(subComplexId);
        subComplex.addParticipant(
                buildComplexParticipant("EBI-3", List.of(buildOrthologXref(ortholog1))));

        String orthologComplexId = "CPX-3";
        IntactComplex orthologComplex = new IntactComplex("test");
        orthologComplex.assignComplexAc(orthologComplexId);
        orthologComplex.addParticipant(new IntactModelledParticipant(subComplex));
        orthologComplex.addParticipant(
                buildComplexParticipant("EBI-6", List.of(buildOrthologXref(ortholog2))));

        String noOrthologComplexId = "CPX-4";
        IntactComplex noOrthologComplex = new IntactComplex("test");
        noOrthologComplex.assignComplexAc(noOrthologComplexId);
        noOrthologComplex.addParticipant(new IntactModelledParticipant(subComplex));
        noOrthologComplex.addParticipant(
                buildComplexParticipant("EBI-8", List.of(buildOrthologXref("other-ortholog"))));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, subComplex), List.of(orthologComplex, noOrthologComplex), List.of())
                .when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertEquals(1, orthologs.size());
        Assert.assertEquals(orthologComplex, orthologs.iterator().next());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(3)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(3)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(3)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologWithInteractorPool() {
        String ortholog1 = "ortholog-1";
        String ortholog2 = "ortholog-2";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));
        complex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog2))));

        String orthologComplexId = "CPX-2";
        IntactComplex orthologComplex = new IntactComplex("test");
        orthologComplex.assignComplexAc(orthologComplexId);
        orthologComplex.addParticipant(
                buildComplexParticipantWithInteractorPool("EBI-3", List.of(buildOrthologXref(ortholog1))));
        orthologComplex.addParticipant(
                buildComplexParticipant("EBI-4", List.of(buildOrthologXref(ortholog2))));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex), List.of())
                .when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertEquals(1, orthologs.size());
        Assert.assertEquals(orthologComplex, orthologs.iterator().next());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologFilterByTaxId() {
        Organism organism = new DefaultOrganism(9606);
        String ortholog1 = "ortholog-1";
        String ortholog2 = "ortholog-2";
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex("test");
        complex.assignComplexAc(complexId);
        complex.addParticipant(
                buildComplexParticipant("EBI-1", List.of(buildOrthologXref(ortholog1))));
        complex.addParticipant(
                buildComplexParticipant("EBI-2", List.of(buildOrthologXref(ortholog2))));
        complex.setOrganism(organism);

        String orthologComplexId1 = "CPX-2";
        IntactComplex orthologComplex1 = new IntactComplex("test");
        orthologComplex1.assignComplexAc(orthologComplexId1);
        orthologComplex1.addParticipant(
                buildComplexParticipant("EBI-3", List.of(buildOrthologXref(ortholog1))));
        orthologComplex1.addParticipant(
                buildComplexParticipant("EBI-4", List.of(buildOrthologXref(ortholog2))));
        orthologComplex1.setOrganism(organism);

        String orthologComplexId2 = "CPX-3";
        IntactComplex orthologComplex2 = new IntactComplex("test");
        orthologComplex2.assignComplexAc(orthologComplexId2);
        orthologComplex2.addParticipant(
                buildComplexParticipant("EBI-5", List.of(buildOrthologXref(ortholog1))));
        orthologComplex2.addParticipant(
                buildComplexParticipant("EBI-6", List.of(buildOrthologXref(ortholog2))));
        orthologComplex2.setOrganism(new DefaultOrganism(20));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex, orthologComplex1, orthologComplex2), List.of())
                .when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, organism.getTaxId());

        Assert.assertEquals(1, orthologs.size());
        Assert.assertEquals(orthologComplex1, orthologs.iterator().next());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao, Mockito.times(2)).getEntityManager();
        Mockito.verify(entityManager, Mockito.times(2)).createQuery(Mockito.anyString());
        Mockito.verify(query, Mockito.times(2)).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    @Test
    public void findComplexOrthologsNoComplexesFound() {
        String complexId = "CPX-1";
        IntactComplex complex = new IntactComplex(complexId);
        complex.addParticipant(buildComplexParticipant("EBI-1", List.of()));

        Mockito.doReturn(complex).when(complexDao).getLatestComplexVersionByComplexAc(Mockito.anyString());
        Mockito.doReturn(List.of(complex), List.of()).when(query).getResultList();

        Collection<IntactComplex> orthologs = complexOrthologFinder.findComplexOrthologs(complexId, null);

        Assert.assertTrue(orthologs.isEmpty());

        Mockito.verify(intactDao).getComplexDao();
        Mockito.verify(intactDao).getEntityManager();
        Mockito.verify(entityManager).createQuery(Mockito.anyString());
        Mockito.verify(query).getResultList();
        Mockito.verify(complexDao).getLatestComplexVersionByComplexAc(complexId);
    }

    private IntactModelledParticipant buildComplexParticipant(
            String interactorAc,
            Collection<Xref> xrefs) {

        CvTerm interactorTypeCv = new DefaultCvTerm("");
        interactorTypeCv.setFullName("test type");
        interactorTypeCv.setMIIdentifier("MI:1");
        IntactProtein interactor = new IntactProtein("test", interactorTypeCv);
        interactor.setAc(interactorAc);
        interactor.setGeneName("test gene");
        interactor.setFullName("test name");
        interactor.getXrefs().addAll(xrefs);
        return new IntactModelledParticipant(interactor);
    }

    private IntactModelledParticipant buildComplexParticipantWithInteractorPool(
            String interactorAc,
            Collection<Xref> xrefs) {

        CvTerm interactorTypeCv = new DefaultCvTerm("");
        interactorTypeCv.setFullName("test type");
        interactorTypeCv.setMIIdentifier("MI:1");
        IntactProtein interactor = new IntactProtein("test", interactorTypeCv);
        interactor.setAc(interactorAc);
        interactor.setGeneName("test gene");
        interactor.setFullName("test name");
        interactor.getXrefs().addAll(xrefs);
        InteractorPool interactorPool = new IntactInteractorPool("test pool", new DefaultCvTerm(""));
        interactorPool.add(interactor);
        return new IntactModelledParticipant(interactorPool);
    }

    private Xref buildOrthologXref(String xrefId) {
        CvTerm database = new DefaultCvTerm("test db");
        CvTerm qualifier = new DefaultCvTerm("orthology group");
        qualifier.setMIIdentifier(ComplexOrthologFinder.ORTHOLOGY_MI);
        return new DefaultXref(database, xrefId, qualifier);
    }
}

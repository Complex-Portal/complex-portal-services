package uk.ac.ebi.complex.service.ortholog.processor;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Alias;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.AliasUtils;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.finder.ComplexOrthologFinder;
import uk.ac.ebi.complex.service.ortholog.model.ComplexOrthologs;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@AllArgsConstructor
public class ComplexOrthologsProcessor implements ItemProcessor<IntactComplex, ComplexOrthologs>, ItemStream {

    private static final Set<LifeCycleStatus> STATUSES_TO_CONSIDER = Set.of(
            LifeCycleStatus.NEW,
            LifeCycleStatus.CURATION_IN_PROGRESS,
            LifeCycleStatus.READY_FOR_CHECKING,
            LifeCycleStatus.READY_FOR_RELEASE,
            LifeCycleStatus.RELEASED);

    private static final String GO_MI_REF = "MI:0448";
    private static final String CELLULAR_COMPONENT_QUALIFIER_NAME = "cellular component";

    private final ComplexOrthologFinder complexOrthologFinder;
    private final String taxId;

    @Override
    public ComplexOrthologs process(IntactComplex item) {
        if (!item.isPredictedComplex() && STATUSES_TO_CONSIDER.contains(item.getStatus())) {
            Collection<IntactComplex> complexes = this.complexOrthologFinder.findComplexOrthologs(
                    item.getComplexAc(), Integer.valueOf(taxId));

            return ComplexOrthologs.builder()
                    .inputComplex(mapComplex(item))
                    .outputComplexes(complexes.stream().map(this::mapComplex).collect(Collectors.toList()))
                    .build();
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
    }

    private Collection<Xref> getGoXrefs(IntactComplex complex) {
        return XrefUtils.collectAllXrefsHavingDatabase(complex.getXrefs(), GO_MI_REF, null);
    }

    private Collection<String> filterXrefsWithQualifierFullName(Collection<Xref> xrefs, String qualifierFullName) {
        return xrefs.stream()
                .filter(xref -> xref.getQualifier() != null)
                .filter(xref -> qualifierFullName.equals(xref.getQualifier().getFullName()))
                .map(Xref::getId)
                .collect(Collectors.toList());
    }

    private ComplexOrthologs.ComplexWithXrefs mapComplex(IntactComplex complex) {
        Collection<Xref> goXrefs = getGoXrefs(complex);
        return ComplexOrthologs.ComplexWithXrefs.builder()
                .complexId(complex.getComplexAc())
                .complexName(getComplexName(complex))
                .predicted(complex.isPredictedComplex())
                .cellularComponents(filterXrefsWithQualifierFullName(goXrefs, CELLULAR_COMPONENT_QUALIFIER_NAME))
                .build();
    }

    private String getComplexName(IntactComplex complex) {
        String name = complex.getRecommendedName();
        if (name != null) return name;
        name = complex.getSystematicName();
        if (name != null) return name;
        List<String> synonyms = getComplexSynonyms(complex);
        if (!synonyms.isEmpty()) return synonyms.get(0);
        return complex.getShortName();
    }

    private List<String> getComplexSynonyms(IntactComplex complex) {
        List<String> synosyms = new ArrayList<>();
        for (Alias alias : AliasUtils.collectAllAliasesHavingType(complex.getAliases(), Alias.COMPLEX_SYNONYM_MI, Alias.COMPLEX_SYNONYM)) {
            synosyms.add(alias.getName());
        }
        return synosyms;
    }
}

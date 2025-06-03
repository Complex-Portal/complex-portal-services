package uk.ac.ebi.complex.service.ortholog.processor;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.finder.ComplexOrthologFinder;
import uk.ac.ebi.complex.service.ortholog.model.ComplexOrthologs;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.util.Collection;
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

    private final ComplexOrthologFinder complexOrthologFinder;
    private final String taxId;

    @Override
    public ComplexOrthologs process(IntactComplex item) {
        if (STATUSES_TO_CONSIDER.contains(item.getStatus())) {
            Collection<IntactComplex> complexes = this.complexOrthologFinder.findComplexOrthologs(
                    item.getComplexAc(), Integer.valueOf(taxId));
            return ComplexOrthologs.builder()
                    .inputComplexId(item.getComplexAc())
                    .outputComplexIds(complexes.stream().map(IntactComplex::getComplexAc).collect(Collectors.toList()))
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
}

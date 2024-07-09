package uk.ac.ebi.complex.service.writer;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.model.UniplexComplex;

import java.util.List;

public class UniplexComplexWriter implements ItemWriter<UniplexComplex>, ItemStream  {

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {

    }

    @Override
    public void write(List<? extends UniplexComplex> items) throws Exception {
        // TODO: write complexes to DB

        // If null, ignore
        // The processor returns null when there are multiple exact matches

        // If existingComplex => merge uniplex complex with existing complex
        // - new Xref for each cluster if
        // - xrefs need to have an ECO code associated (we already do this for GO xrefs)
        // - new annotation for the confidence

        // Else -> new complex from scratch
        // - xref for each cluster if
        // - xrefs need to have an ECO code associated
        // - annotation for the confidence
        // - participants for each protei
        // - new interactor may need to be imported from UniProt and created
        // - complex ac - same sequence as curated complexes
        // - short label can be anything
        // - systematic name using protein gene names
        // - Complex type = stable complex
        // - Interaction type = physical association
    }
}

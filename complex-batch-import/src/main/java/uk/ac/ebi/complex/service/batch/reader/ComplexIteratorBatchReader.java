package uk.ac.ebi.complex.service.batch.reader;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.util.Iterator;

@RequiredArgsConstructor
public class ComplexIteratorBatchReader implements ItemReader<IntactComplex>, ItemStream {

    private final ComplexService complexService;

    private Iterator<Complex> complexIterator;

    @Override
    public IntactComplex read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Complex nextObject = complexIterator.hasNext() ? complexIterator.next() : null;
        return (IntactComplex) nextObject;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        this.complexIterator = complexService.iterateAll();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {

    }
}

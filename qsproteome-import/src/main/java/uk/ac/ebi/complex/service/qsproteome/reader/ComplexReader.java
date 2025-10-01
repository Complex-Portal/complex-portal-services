package uk.ac.ebi.complex.service.qsproteome.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.util.Iterator;

@Log4j
@Component
@RequiredArgsConstructor
public class ComplexReader implements ItemReader<IntactComplex>, ItemStream {

    private final IntactDao intactDao;
    private final ComplexService complexService;
    private Iterator<Complex> complexIterator;

    @Override
    public IntactComplex read() {
        if (complexIterator.hasNext()) {
            Complex complex = complexIterator.next();
            return (IntactComplex) intactDao.getEntityManager().merge(complex);
        }
        return null;
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

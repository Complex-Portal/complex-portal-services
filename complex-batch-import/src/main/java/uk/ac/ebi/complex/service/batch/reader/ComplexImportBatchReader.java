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
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

@RequiredArgsConstructor
public class ComplexImportBatchReader<T, R extends ComplexToImport<T>> implements ItemReader<R>, ItemStream {

    private static final String COUNT_OPTION = "complex_count";

    private final FileConfiguration fileConfiguration;
    private final ComplexFileReader<T, R> complexFileReader;

    private Iterator<R> complexIterator;
    private int complexCount = 0;

    @Override
    public R read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        R nextObject = complexIterator.hasNext() ? complexIterator.next() : null;
        this.complexCount++;
        return nextObject;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            // TODO: should we have a filter on the confidence score?
            File inputFile = fileConfiguration.outputPath().toFile();
            Collection<R> complexes = complexFileReader.readComplexesFromFile(inputFile);
            this.complexIterator = complexes.iterator();

            // the job has been restarted, we update iterator
            if (executionContext.containsKey(COUNT_OPTION)){
                this.complexCount = executionContext.getInt(COUNT_OPTION);

                int count = 0;
                while (count < this.complexCount && this.complexIterator.hasNext()) {
                    this.complexIterator.next();
                    count++;
                }
            }
        } catch (IOException e) {
            throw new ItemStreamException("Input file could not be read: " + fileConfiguration.getInputFileName(), e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        executionContext.put(COUNT_OPTION, complexCount);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}

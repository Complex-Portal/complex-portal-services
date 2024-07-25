package uk.ac.ebi.complex.service.reader;

import lombok.Data;
import org.springframework.batch.item.*;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.config.ComplexServiceConfiguration;
import uk.ac.ebi.complex.service.model.UniplexCluster;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

@Data
public class UniplexFileReader implements ItemReader<UniplexCluster>, ItemStream {

    private static final String COUNT_OPTION = "cluster_count";

    private ComplexServiceConfiguration config;

    private final UniplexClusterReader uniplexClusterReader;

    private Iterator<UniplexCluster> clusterIterator;

    private int clusterCount = 0;

    @Override
    public UniplexCluster read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        UniplexCluster nextObject = clusterIterator.hasNext() ? clusterIterator.next() : null;
        this.clusterCount++;
        return nextObject;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            // TODO: should we have a filter on the confidence score?
            Collection<UniplexCluster> clusters = uniplexClusterReader.readClustersFromFile();
            this.clusterIterator = clusters.iterator();

            // the job has been restarted, we update iterator
            if (executionContext.containsKey(COUNT_OPTION)){
                this.clusterCount = executionContext.getInt(COUNT_OPTION);

                int count = 0;
                while (count < this.clusterCount && this.clusterIterator.hasNext()) {
                    this.clusterIterator.next();
                    count++;
                }
            }
        } catch (IOException e) {
            throw new ItemStreamException("Input file could not be read: " + config.getInputFileName(), e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        executionContext.put(COUNT_OPTION, clusterCount);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}

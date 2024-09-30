package uk.ac.ebi.complex.service.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.config.AppProperties;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.logging.WriteReportWriter;
import uk.ac.ebi.complex.service.manager.UniplexComplexManager;
import uk.ac.ebi.complex.service.model.UniplexComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;
import uk.ac.ebi.intact.jami.synchronizer.listener.impl.DbSynchronizerStatisticsReporter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@Component
@RequiredArgsConstructor
public class UniplexComplexWriter implements ItemWriter<UniplexComplex>, ItemStream {

    public final static String PERSIST_MAP_COUNT = "persisted.map";
    public final static String MERGE_MAP_COUNT = "merged.map";
    public final static String DELETED_MAP_COUNT = "deleted.map";
    public final static String MERGED_TRANSIENT_MAP_COUNT = "merged.transient.map";
    public final static String REPLACED_TRANSIENT_MAP_COUNT = "transient.replaced.map";

    private final UniplexComplexManager uniplexComplexManager;
    private final ComplexService complexService;
    private final FileConfiguration fileConfiguration;
    private final AppProperties appProperties;

    private DbSynchronizerStatisticsReporter synchronizerListener;

    private WriteReportWriter newComplexesReportWriter;
    private WriteReportWriter mergedComplexesReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file  writer could not be opened", e);
        }

        this.synchronizerListener = new DbSynchronizerStatisticsReporter();

        // restore previous statistics
        if (!executionContext.isEmpty()){
            for (Map.Entry<String, Object> entry : executionContext.entrySet()){
                if (entry.getKey().startsWith(PERSIST_MAP_COUNT+"_")){
                    try {
                        this.synchronizerListener.getPersistedCounts()
                                .put(Class.forName(entry.getKey().substring(PERSIST_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
                    } catch (ClassNotFoundException e) {
                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
                    }
                }
                else if (entry.getKey().startsWith(MERGE_MAP_COUNT+"_")){
                    try {
                        this.synchronizerListener.getMergedCounts()
                                .put(Class.forName(entry.getKey().substring(MERGE_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
                    } catch (ClassNotFoundException e) {
                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
                    }
                }
                else if (entry.getKey().startsWith(DELETED_MAP_COUNT+"_")){
                    try {
                        this.synchronizerListener.getDeletedCounts()
                                .put(Class.forName(entry.getKey().substring(DELETED_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
                    } catch (ClassNotFoundException e) {
                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
                    }
                }
                else if (entry.getKey().startsWith(MERGED_TRANSIENT_MAP_COUNT+"_")){
                    try {
                        this.synchronizerListener.getMergedTransientCounts()
                                .put(Class.forName(entry.getKey().substring(MERGED_TRANSIENT_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
                    } catch (ClassNotFoundException e) {
                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
                    }
                }
                else if (entry.getKey().startsWith(REPLACED_TRANSIENT_MAP_COUNT+"_")){
                    try {
                        this.synchronizerListener.getTransientReplacedCounts()
                                .put(Class.forName(entry.getKey().substring(REPLACED_TRANSIENT_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
                    } catch (ClassNotFoundException e) {
                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
                    }
                }
            }
        }

        this.complexService.getIntactDao().getSynchronizerContext().initialiseDbSynchronizerListener(synchronizerListener);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.newComplexesReportWriter.flush();
            this.mergedComplexesReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }

        // persist statistics
        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getPersistedCounts().entrySet()){
            executionContext.put(PERSIST_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
        }
        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getMergedCounts().entrySet()){
            executionContext.put(MERGE_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
        }
        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getTransientReplacedCounts().entrySet()){
            executionContext.put(REPLACED_TRANSIENT_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
        }
        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getMergedTransientCounts().entrySet()){
            executionContext.put(MERGED_TRANSIENT_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
        }
        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getDeletedCounts().entrySet()){
            executionContext.put(DELETED_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.newComplexesReportWriter.close();
            this.mergedComplexesReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }

        // flush statistics
        log.info("Created objects in database: ");
        for (Map.Entry<Class, Integer> entry : synchronizerListener.getPersistedCounts().entrySet()){
            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
        }
        log.info("Objects already in the database: ");
        for (Map.Entry<Class, Integer> entry : synchronizerListener.getMergedTransientCounts().entrySet()){
            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
        }
        log.info("Objects replaced with existing instance in the database: ");
        for (Map.Entry<Class, Integer> entry : synchronizerListener.getTransientReplacedCounts().entrySet()){
            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
        }
        log.info("Existing Objects merged: ");
        for (Map.Entry<Class, Integer> entry : synchronizerListener.getMergedCounts().entrySet()){
            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
        }
        // remove listener
        this.synchronizerListener = null;
    }

    @Override
    public void write(List<? extends UniplexComplex> items) throws Exception {
        Map<String, IntactComplex> complexesToSave = new HashMap<>();
        for (UniplexComplex complex: items) {
            try {
                if (complex.getExistingComplex() != null) {
                    complexesToSave.put(
                            String.join(",", complex.getCluster().getClusterIds()),
                            uniplexComplexManager.mergeClusterWithExistingComplex(
                                    complex.getCluster(),
                                    complex.getExistingComplex()));
                } else {
                    complexesToSave.put(
                            String.join(",", complex.getCluster().getClusterIds()),
                            uniplexComplexManager.newComplexFromCluster(complex.getCluster()));
                }
            } catch (Exception e) {
                log.error("Error writing to DB uniplex clusters: " + String.join(",", complex.getCluster().getClusterIds()), e);
                errorReportWriter.write(complex.getCluster().getClusterIds(), e.getMessage());
            }
        }

        if (!appProperties.isDryRunMode()) {
            this.complexService.saveOrUpdate(complexesToSave.values());
        }

        for (UniplexComplex complex: items) {
            String clusterIds = String.join(",", complex.getCluster().getClusterIds());
            if (complexesToSave.containsKey(clusterIds)) {
                if (complex.getExistingComplex() != null) {
                    mergedComplexesReportWriter.write(
                            complex.getCluster().getClusterIds(),
                            complex.getCluster().getClusterConfidence(),
                            complex.getCluster().getUniprotAcs(),
                            complexesToSave.get(clusterIds).getComplexAc());
                } else {
                    newComplexesReportWriter.write(
                            complex.getCluster().getClusterIds(),
                            complex.getCluster().getClusterConfidence(),
                            complex.getCluster().getUniprotAcs(),
                            complexesToSave.get(clusterIds).getComplexAc());
                }
            }
        }
    }

    private void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.newComplexesReportWriter = new WriteReportWriter(new File(reportDirectory, "new_complexes" + extension), separator, header);
        this.mergedComplexesReportWriter = new WriteReportWriter(new File(reportDirectory, "merged_complexes" + extension), separator, header);
        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }
}

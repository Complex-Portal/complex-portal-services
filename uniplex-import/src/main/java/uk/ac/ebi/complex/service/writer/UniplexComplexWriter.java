package uk.ac.ebi.complex.service.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.config.AppProperties;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.config.UniplexFileConfiguration;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.logging.WriteReportWriter;
import uk.ac.ebi.complex.service.manager.UniplexComplexManager;
import uk.ac.ebi.complex.service.model.UniplexComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@Component
@RequiredArgsConstructor
public class UniplexComplexWriter extends AbstractBatchWriter<UniplexComplex> {

    private final UniplexComplexManager uniplexComplexManager;
    private final ComplexService complexService;
    private final UniplexFileConfiguration fileConfiguration;
    private final AppProperties appProperties;

    private WriteReportWriter newComplexesReportWriter;
    private WriteReportWriter mergedComplexesReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        try {
            this.newComplexesReportWriter.flush();
            this.mergedComplexesReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        try {
            this.newComplexesReportWriter.close();
            this.mergedComplexesReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
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

    @Override
    protected FileConfiguration getFileConfiguration() {
        return fileConfiguration;
    }

    @Override
    protected ComplexService getComplexService() {
        return complexService;
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.newComplexesReportWriter = new WriteReportWriter(new File(reportDirectory, "new_complexes" + extension), separator, header);
        this.mergedComplexesReportWriter = new WriteReportWriter(new File(reportDirectory, "merged_complexes" + extension), separator, header);
        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }
}

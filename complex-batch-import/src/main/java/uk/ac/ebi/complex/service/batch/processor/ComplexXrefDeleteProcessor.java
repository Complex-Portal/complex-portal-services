package uk.ac.ebi.complex.service.batch.processor;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.batch.model.ComplexWithXrefsToDelete;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class ComplexXrefDeleteProcessor<T, R extends ComplexToImport<T>> extends AbstractBatchProcessor<IntactComplex, ComplexWithXrefsToDelete> {

    private final ComplexManager<T, R> complexManager;
    private final IntactDao intactDao;
    private final String databaseId;

    private ErrorsReportWriter errorReportWriter;
    private Map<String, Set<String>> complexesByIdentityXrefs;
    private Map<String, Set<String>> complexesBySubsetXrefs;
    private Map<String, Set<String>> complexesByComplexClusterXrefs;

    @Override
    public ComplexWithXrefsToDelete process(IntactComplex item) throws Exception {
        IntactComplex complex = intactDao.getEntityManager().merge(item);

        try {
            Collection<Xref> identityXrefs = complexManager.getXrefs(complex, databaseId, Xref.IDENTITY_MI);
            List<Xref> identityXrefsToDelete = identityXrefs.stream()
                    .filter(xref -> !complexesByIdentityXrefs.containsKey(xref.getId()) ||
                                    !complexesByIdentityXrefs.get(xref.getId()).contains(complex.getComplexAc()))
                    .collect(Collectors.toList());

            Collection<Xref> subsetXrefs = complexManager.getXrefs(complex, databaseId, ComplexManager.SUBSET_QUALIFIER_MI);
            List<Xref> subsetXrefsToDelete = subsetXrefs.stream()
                    .filter(xref -> !complexesBySubsetXrefs.containsKey(xref.getId()) ||
                            !complexesBySubsetXrefs.get(xref.getId()).contains(complex.getComplexAc()))
                    .collect(Collectors.toList());

            Collection<Xref> complexClusterXrefs = complexManager.getXrefs(complex, databaseId, ComplexManager.COMPLEX_CLUSTER_QUALIFIER_MI);
            List<Xref> complexClusterXrefsToDelete = complexClusterXrefs.stream()
                    .filter(xref -> !complexesByComplexClusterXrefs.containsKey(xref.getId()) ||
                            !complexesByComplexClusterXrefs.get(xref.getId()).contains(complex.getComplexAc()))
                    .collect(Collectors.toList());

            if (identityXrefsToDelete.isEmpty() && subsetXrefsToDelete.isEmpty() && complexClusterXrefsToDelete.isEmpty()) {
                return null;
            }

            return new ComplexWithXrefsToDelete(
                    complex,
                    identityXrefsToDelete,
                    subsetXrefsToDelete,
                    complexClusterXrefsToDelete);

        } catch (Exception e) {
            log.error("Error checking xrefs to delete for complex : " + complex.getComplexAc(), e);
            errorReportWriter.write(List.of(complex.getComplexAc()), e.getMessage());
            return null;
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        super.open(executionContext);

        this.complexesByIdentityXrefs = new HashMap<>();
        this.complexesBySubsetXrefs = new HashMap<>();
        this.complexesByComplexClusterXrefs = new HashMap<>();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String extension = fileConfiguration.getExtension();
        try {
            readComplexesFromFile(new File(reportDirectory, "complexes_to_update" + extension));
            readComplexesFromFile(new File(reportDirectory, "complexes_unchanged" + extension));
        } catch (IOException e) {
            throw new ItemStreamException("Report file could not be opened", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String sep = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.errorReportWriter = new ErrorsReportWriter(
                new File(reportDirectory, "delete_process_errors" + extension), sep, header);
    }

    private void readComplexesFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();

        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        csvReader.forEach(csvLine -> {
            if (csvLine.length > 1 || (csvLine.length == 1 && !csvLine[0].isEmpty())) {
                String id = csvLine[0];
                String[] complexIds = csvLine[2].split(" ");
                String qualifier = csvLine[3];
                for (String complexId : complexIds) {
                    if (Xref.IDENTITY.equals(qualifier)) {
                        complexesByIdentityXrefs.putIfAbsent(id, new HashSet<>());
                        complexesByIdentityXrefs.get(id).add(complexId);
                    } else if (ComplexManager.SUBSET_QUALIFIER.equals(qualifier)) {
                        complexesBySubsetXrefs.putIfAbsent(id, new HashSet<>());
                        complexesBySubsetXrefs.get(id).add(complexId);
                    } else if (ComplexManager.COMPLEX_CLUSTER_QUALIFIER.equals(qualifier)) {
                        complexesByComplexClusterXrefs.putIfAbsent(id, new HashSet<>());
                        complexesByComplexClusterXrefs.get(id).add(complexId);
                    }
                }
            }
        });
        csvReader.close();
    }
}

package uk.ac.ebi.complex.service.ortholog.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.ortholog.config.FileConfiguration;
import uk.ac.ebi.complex.service.ortholog.logging.ReportWriter;
import uk.ac.ebi.complex.service.ortholog.model.ComplexOrthologs;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Log4j
@RequiredArgsConstructor
public class ComplexOrthologsWriter implements ItemWriter<ComplexOrthologs>, ItemStream {

    protected final FileConfiguration fileConfiguration;

    private ReportWriter complexesWithNoOrthologs;
    private ReportWriter complexesWithOrthologsDifferentCellularComponents;
    private ReportWriter complexesWithOrthologsSameCellularComponents;

    @Override
    public void write(List<? extends ComplexOrthologs> items) {
        for (ComplexOrthologs item : items) {
            if (item.getOutputComplexes().isEmpty()) {
                complexesWithNoOrthologs.writeLine(new String[]{
                        item.getInputComplex().getComplexId(),
                        prepareString(item.getInputComplex().getComplexName()),
                        String.valueOf(item.getInputComplex().isPredicted()),
                        String.join("|", item.getInputComplex().getCellularComponents())
                });
            } else {
                for (ComplexOrthologs.ComplexWithXrefs complexWithXref : item.getOutputComplexes()) {
                    if (doComplexesHaveCommonCellularComponent(item.getInputComplex(), complexWithXref)) {
                        complexesWithOrthologsSameCellularComponents.writeLine(new String[]{
                                item.getInputComplex().getComplexId(),
                                prepareString(item.getInputComplex().getComplexName()),
                                String.valueOf(item.getInputComplex().isPredicted()),
                                String.join("|", item.getInputComplex().getCellularComponents()),
                                complexWithXref.getComplexId(),
                                prepareString(complexWithXref.getComplexName()),
                                String.valueOf(complexWithXref.isPredicted()),
                                String.join("|", complexWithXref.getCellularComponents())
                        });
                    } else {
                        complexesWithOrthologsDifferentCellularComponents.writeLine(new String[]{
                                item.getInputComplex().getComplexId(),
                                prepareString(item.getInputComplex().getComplexName()),
                                String.valueOf(item.getInputComplex().isPredicted()),
                                String.join("|", item.getInputComplex().getCellularComponents()),
                                complexWithXref.getComplexId(),
                                prepareString(complexWithXref.getComplexName()),
                                String.valueOf(complexWithXref.isPredicted()),
                                String.join("|", complexWithXref.getCellularComponents())
                        });
                    }
                }
            }
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be opened", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.complexesWithNoOrthologs.flush();
            this.complexesWithOrthologsDifferentCellularComponents.flush();
            this.complexesWithOrthologsSameCellularComponents.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.complexesWithNoOrthologs.flush();
            this.complexesWithNoOrthologs.close();
            this.complexesWithOrthologsDifferentCellularComponents.flush();
            this.complexesWithOrthologsDifferentCellularComponents.close();
            this.complexesWithOrthologsSameCellularComponents.flush();
            this.complexesWithOrthologsSameCellularComponents.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    protected void initialiseReportWriters() throws IOException {
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

        File noOrthologsFile = new File(reportDirectory, "no_orthologs" + extension);
        this.complexesWithNoOrthologs = new ReportWriter(noOrthologsFile, separator, header, new String[]{
                "complex_id",
                "complex_name",
                "is_predicted",
                "cellular_components"
        });
        File withOrthologsDifferentCellularComponentFile = new File(reportDirectory, "with_orthologs_different_cellular_components" + extension);
        this.complexesWithOrthologsDifferentCellularComponents = new ReportWriter(withOrthologsDifferentCellularComponentFile, separator, header, new String[]{
                "complex_id",
                "complex_name",
                "is_predicted",
                "cellular_components",
                "ortholog_complex_id",
                "ortholog_complex_name",
                "ortholog_is_predicted",
                "ortholog_cellular_components"
        });
        File withOrthologsSameCellularComponentsFile = new File(reportDirectory, "with_orthologs_same_cellular_components" + extension);
        this.complexesWithOrthologsSameCellularComponents = new ReportWriter(withOrthologsSameCellularComponentsFile, separator, header, new String[]{
                "complex_id",
                "complex_name",
                "is_predicted",
                "cellular_components",
                "ortholog_complex_id",
                "ortholog_complex_name",
                "ortholog_is_predicted",
                "ortholog_cellular_components"
        });
    }

    private boolean doComplexesHaveCommonCellularComponent(
            ComplexOrthologs.ComplexWithXrefs complexA,
            ComplexOrthologs.ComplexWithXrefs complexB) {

        if (complexA.getCellularComponents().isEmpty() || complexB.getCellularComponents().isEmpty()) {
            return false;
        }

        return complexA.getCellularComponents().stream()
                .anyMatch(cellularComponent -> complexB.getCellularComponents().contains(cellularComponent));
    }

    private String prepareString(String value) {
        if (value != null && value.isEmpty()) {
            value = value.replaceAll("\n", " ");
            value = value.replaceAll("\t", " ");
            value = value.replaceAll("\\s+", " ");
        }
        return value;
    }
}

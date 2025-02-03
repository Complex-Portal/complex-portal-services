package uk.ac.ebi.complex.service.batch.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.AllArgsConstructor;
import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

@AllArgsConstructor
@SuperBuilder
public abstract class ComplexFileWriter<T, R extends ComplexToImport<T>> {

    private final FileConfiguration fileConfiguration;

    public void writeComplexesToFile(Collection<R> complexes) throws IOException {
        File outputFile = fileConfiguration.outputPath().toFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(fileConfiguration.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (fileConfiguration.isHeader()) {
            csvWriter.writeNext(headerLine());
        }
        complexes.forEach(complex -> csvWriter.writeNext(complexToStringArray(complex)));
        csvWriter.close();
    }

    protected abstract String[] headerLine();

    protected abstract String[] complexToStringArray(R complex);
}

package uk.ac.ebi.complex.service.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.AssemblyEntry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesFileWriter {

    private final FileConfiguration fileConfiguration;

    public void writeAssembliesToFile(Collection<AssemblyEntry> assemblies) throws IOException {
        File outputFile = fileConfiguration.outputPath().toFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(fileConfiguration.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (fileConfiguration.isHeader()) {
            csvWriter.writeNext(new String[]{"assembly", "complex_id", "uniprot_acs"});
        }
        assemblies.forEach(assembly -> csvWriter.writeNext(assemblyToStringArray(assembly)));
        csvWriter.close();
    }

    private String[] assemblyToStringArray(AssemblyEntry assemblyEntry) {
        String assemblies = String.join(" ", assemblyEntry.getAssemblies());
        String complexIds = String.join(" ", assemblyEntry.getComplexIds());
        String uniprotAcs = proteinsToString(assemblyEntry);
        return new String[]{assemblies, complexIds, uniprotAcs};

    }

    private String proteinsToString(AssemblyEntry assemblyEntry) {
        return assemblyEntry.getProteins().stream()
                .map(protein -> protein.getProteinAc() + "|" + protein.getOrganism())
                .collect(Collectors.joining(" "));
    }
}

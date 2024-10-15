package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ProcessReportWriter extends ReportWriter {

    public static final String[] NO_CHANGES_HEADER_LINE = new String[]{
            "complex_id", "assemblies" };
    public static final String[] CHANGES_HEADER_LINE = new String[]{
            "complex_id", "assemblies", "assemblies_to_keep", "assemblies_to_add", "assemblies_to_remove", "assemblies_to_update" };

    public ProcessReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(String complexId,
                      Collection<String> assemblies) throws IOException {

        csvWriter.writeNext(new String[]{
                complexId,
                String.join(" ", assemblies)
        });
        flush();
    }

    public void write(String complexId,
                      Collection<String> assemblies,
                      Collection<String> assembliesToKeep,
                      Collection<String> assembliesToAdd,
                      Collection<String> assembliesToRemove,
                      Collection<String> assembliesToUpdate) throws IOException {

        csvWriter.writeNext(new String[]{
                complexId,
                String.join(" ", assemblies),
                String.join(" ", assembliesToKeep),
                String.join(" ", assembliesToAdd),
                String.join(" ", assembliesToRemove),
                String.join(" ", assembliesToUpdate)
        });
        flush();
    }
}

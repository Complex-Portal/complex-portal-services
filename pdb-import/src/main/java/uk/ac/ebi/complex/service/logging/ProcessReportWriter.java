package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ProcessReportWriter extends ReportWriter {

    public static final String[] NO_CHANGES_HEADER_LINE = new String[]{
            "complex_id", "assemblies_from_pdb" };
    public static final String[] COMPLEXES_WITH_ASSEMBLIES_TO_ADD = new String[]{
            "complex_id", "assemblies_from_pdb", "xrefs_to_add" };
    public static final String[] COMPLEXES_WITH_ASSEMBLIES_TO_REMOVE = new String[]{
            "complex_id", "assemblies_from_pdb", "xrefs_to_remove" };
    public static final String[] COMPLEXES_WITH_ASSEMBLIES_TO_UPDATE = new String[]{
            "complex_id", "assemblies_from_pdb", "xrefs_to_update" };
    public static final String[] COMPLEXES_WITH_XREFS_TO_REVIEW = new String[]{
            "complex_id", "assemblies_from_pdb", "xrefs_to_add", "xrefs_to_remove", "xrefs_to_update", "exp_evidence_xrefs_to_review" };

    public ProcessReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(String complexId,
                      Collection<String> assemblies) throws IOException {

        writeLine(new String[]{
                complexId,
                String.join(" ", assemblies)
        });
        flush();
    }

    public void write(String complexId,
                      Collection<String> assemblies,
                      Collection<String> assembliesToUpdate) throws IOException {

        writeLine(new String[]{
                complexId,
                String.join(" ", assemblies),
                String.join(" ", assembliesToUpdate)
        });
        flush();
    }

    public void write(String complexId,
                      Collection<String> assemblies,
                      Collection<String> assembliesToAdd,
                      Collection<String> assembliesToRemove,
                      Collection<String> assembliesToUpdate,
                      Collection<String> assembliesToReview) throws IOException {

        writeLine(new String[]{
                complexId,
                String.join(" ", assemblies),
                String.join(" ", assembliesToAdd),
                String.join(" ", assembliesToRemove),
                String.join(" ", assembliesToUpdate),
                String.join(" ", assembliesToReview)
        });
        flush();
    }
}

package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;

@SuperBuilder
public class MusicComplexWriter extends ComplexFileWriter<Double, MusicComplexToImport> {

    @Override
    protected String[] headerLine() {
        return new String[]{"name", "ids", "uniprot_acs", "confidence"};
    }

    @Override
    protected String[] complexToStringArray(MusicComplexToImport complex) {
        String name = complex.getName();
        String ids = String.join(" ", complex.getComplexIds());
        String robustness = complex.getConfidence().toString();
        String proteinIds = String.join(" ", complex.getProteinIds());
        return new String[]{name, ids, proteinIds, robustness};
    }
}

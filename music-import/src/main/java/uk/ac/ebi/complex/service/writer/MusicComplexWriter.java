package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;

@SuperBuilder
public class MusicComplexWriter extends ComplexFileWriter<Double, MusicComplexToImport> {

    @Override
    protected String[] complexToStringArray(MusicComplexToImport complex) {
        String ids = String.join(" ", complex.getComplexIds());
        String robustness = complex.getConfidence().toString();
        String proteinIds = String.join(" ", complex.getProteinIds());
        return new String[]{ids, proteinIds, robustness};
    }
}

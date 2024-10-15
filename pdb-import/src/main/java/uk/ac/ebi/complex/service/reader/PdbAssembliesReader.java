package uk.ac.ebi.complex.service.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblies;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import javax.transaction.Transactional;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Log4j
@Component
@RequiredArgsConstructor
@Transactional
public class PdbAssembliesReader implements ItemReader<ComplexWithAssemblies>, ItemStream {

    private static final String WWPDB_DB_MI = "MI:0805";
    private static final String WWPDB_DB_NAME = "wwpdb";

    private final ComplexService complexService;
    private final FileConfiguration fileConfiguration;
    private final PdbAssembliesFileReader pdbAssembliesFileReader;

    private Iterator<Complex> complexIterator;
    private Map<String, Set<String>> complexAndAssemblies;

    @Override
    public ComplexWithAssemblies read() {
        while (complexIterator.hasNext()) {
            Complex complex = complexIterator.next();
            IntactComplex intactComplex = (IntactComplex) complex;
            try {
                String complexAc = intactComplex.getComplexAc();
                if (complexAndAssemblies.containsKey(complexAc)) {
                    return new ComplexWithAssemblies(complexAc, complexAndAssemblies.get(complexAc));
                }

                if (!XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty() ||
                        !XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty()) {
                    return new ComplexWithAssemblies(complexAc, new HashSet<>());
                }
            } catch (Exception e) {
                log.error("--- DEBUG ---");
                e.printStackTrace();
                log.error("--- DEBUG ---");
            }
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            File inputFile = new File(fileConfiguration.getInputFileName());
            complexAndAssemblies = pdbAssembliesFileReader.readAssembliesFromFile(inputFile);

            log.info("DEBUG: Read " + complexAndAssemblies.size() + " assemblies from " + inputFile.getAbsolutePath());
            String complexId = complexAndAssemblies.keySet().iterator().next();
            log.info("DEBUG: First assembly: complex id = " + complexId + " , assemblies = " + String.join("|", complexAndAssemblies.get(complexId)));

            this.complexIterator = complexService.iterateAll();
        } catch (IOException e) {
            throw new ItemStreamException("Input file could not be read: " + fileConfiguration.getInputFileName(), e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {

    }
}

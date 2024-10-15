package uk.ac.ebi.complex.service.reader;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblies;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class PdbAssembliesReader implements ItemReader<ComplexWithAssemblies>, ItemStream {

    private static final String WWPDB_DB_MI = "MI:0805";
    private static final String WWPDB_DB_NAME = "wwpdb";

    private final IntactDao intactDao;
    private final FileConfiguration fileConfiguration;
    private final PdbAssembliesFileReader pdbAssembliesFileReader;

    private Iterator<IntactComplex> complexIterator;
    private Map<String, Set<String>> complexAndAssemblies;

    @Override
    public ComplexWithAssemblies read() {
        while (complexIterator.hasNext()) {
            IntactComplex complex = complexIterator.next();
            if (complexAndAssemblies.containsKey(complex.getComplexAc())) {
                return new ComplexWithAssemblies(complex.getComplexAc(), complexAndAssemblies.get(complex.getComplexAc()));
            }

            if (!XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty() ||
                    !XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty()) {
                return new ComplexWithAssemblies(complex.getComplexAc(), new HashSet<>());
            }
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            File inputFile = fileConfiguration.outputPath().toFile();
            complexAndAssemblies = pdbAssembliesFileReader.readAssembliesFromFile(inputFile);

            List<IntactComplex> complexes = intactDao.getComplexDao().getAll();
            this.complexIterator = complexes.iterator();
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

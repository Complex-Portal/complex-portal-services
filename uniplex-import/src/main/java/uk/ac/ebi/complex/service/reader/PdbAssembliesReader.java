package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblies;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesReader implements ItemReader<ComplexWithAssemblies>, ItemStream {

    private static final String WWPDB_DB_MI = "MI:0805";
    private static final String WWPDB_DB_NAME = "wwpdb";

    private final IntactDao intactDao;
    private final ComplexService complexService;
    private final FileConfiguration fileConfiguration;
//    private final PdbAssembliesFileReader pdbAssembliesFileReader;

    private Iterator<Complex> complexIterator;
    private Map<String, Set<String>> complexAndAssemblies;

    @Override
    @Transactional(readOnly = true, propagation = Propagation.SUPPORTS, value = "jamiTransactionManager")
    public ComplexWithAssemblies read() {
        while (complexIterator.hasNext()) {
            Complex complex = complexIterator.next();
            IntactComplex intactComplex = intactDao.getComplexDao().getByAc(((IntactComplex) complex).getAc());
            String complexAc = intactComplex.getComplexAc();
            if (complexAndAssemblies.containsKey(complexAc)) {
                return new ComplexWithAssemblies(complexAc, complexAndAssemblies.get(complexAc));
            }

            if (!XrefUtils.collectAllXrefsHavingDatabase(intactComplex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty() ||
                    !XrefUtils.collectAllXrefsHavingDatabase(intactComplex.getXrefs(), WWPDB_DB_MI, WWPDB_DB_NAME).isEmpty()) {
                return new ComplexWithAssemblies(complexAc, new HashSet<>());
            }
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            File inputFile = new File(fileConfiguration.getInputFileName());
            complexAndAssemblies = readAssembliesFromFile(inputFile);
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

    private Map<String, Set<String>> readAssembliesFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();


        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        Map<String, Set<String>> complexAssemblies = new HashMap<>();
        csvReader.forEach(csvLine -> {
            String complexId = csvLine[4];
            if (StringUtils.isNotEmpty(complexId)) {
                String assembly = csvLine[7].split("_")[0];
                complexAssemblies.putIfAbsent(complexId, new HashSet<>());
                complexAssemblies.get(complexId).add(assembly);
            }
        });
        csvReader.close();

        return complexAssemblies;
    }
}

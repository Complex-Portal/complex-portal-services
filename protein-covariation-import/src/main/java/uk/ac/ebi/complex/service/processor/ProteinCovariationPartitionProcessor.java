package uk.ac.ebi.complex.service.processor;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ProteinCovariation;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Log4j
@SuperBuilder
public class ProteinCovariationPartitionProcessor extends AbstractBatchProcessor<ProteinCovariation, List<ProteinPairCovariation>> {

    private final int partitionIndex;

    private Map<String, String> uniprotProteinMapping;
    private Set<String> proteinsInIntact;
    private Map<String, Set<String>> complexesInIntact;

    @Override
    public List<ProteinPairCovariation> process(ProteinCovariation item) throws Exception {
        List<ProteinPairCovariation> allProteinCovariationsPairs = expandProteinCovariation(item);
        if (!allProteinCovariationsPairs.isEmpty()) {
            return allProteinCovariationsPairs;
        }
        return null;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        complexesInIntact = new HashMap<>();
        try {
            File reportDirectory = new File(fileConfiguration.getReportDirectory());
            File complexesFile = new File(reportDirectory, "complexes" + fileConfiguration.getExtension());
            BufferedReader reader = new BufferedReader(new FileReader(complexesFile));
            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }
            for (String[] row : csvReader) {
                complexesInIntact.put(row[0], Set.of(row[1].split(";")));
            }
            csvReader.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        proteinsInIntact = new HashSet<>();
        try {
            File reportDirectory = new File(fileConfiguration.getReportDirectory());
            File proteinsFile = new File(reportDirectory, "proteins" + fileConfiguration.getExtension());
            BufferedReader reader = new BufferedReader(new FileReader(proteinsFile));
            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }
            for (String[] row : csvReader) {
                proteinsInIntact.add(row[0]);
            }
            csvReader.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        uniprotProteinMapping = new HashMap<>();
        try {
            File reportDirectory = new File(fileConfiguration.getReportDirectory());
            File uniprotMappingFile = new File(reportDirectory, "uniprot_mapping" + fileConfiguration.getExtension());
            BufferedReader reader = new BufferedReader(new FileReader(uniprotMappingFile));
            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }
            for (String[] row : csvReader) {
                uniprotProteinMapping.put(row[0], row[1]);
            }
            csvReader.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
    }

    protected List<ProteinPairCovariation> expandProteinCovariation(ProteinCovariation proteinCovariation) {
        List<ProteinPairCovariation> pairs = new ArrayList<>();

        for (String proteinA : proteinCovariation.getProteinA()) {
            pairs.addAll(expandProteinCovariationB(proteinCovariation, proteinA));
            if (uniprotProteinMapping.containsKey(proteinA)) {
                String newProteinA = uniprotProteinMapping.get(proteinA);
                if (!proteinCovariation.getProteinA().contains(newProteinA)) {
                    pairs.addAll(expandProteinCovariationB(proteinCovariation, newProteinA));
                }
            }
        }

        return pairs;
    }

    protected List<ProteinPairCovariation> expandProteinCovariationB(ProteinCovariation proteinCovariation, String proteinA) {
        List<ProteinPairCovariation> pairs = new ArrayList<>();

        for (String proteinB : proteinCovariation.getProteinB()) {
            addCovariationIfProteinsPartOfComplex(pairs, proteinA, proteinB, proteinCovariation.getProbability());
            if (uniprotProteinMapping.containsKey(proteinB)) {
                String newProteinB = uniprotProteinMapping.get(proteinB);
                if (!proteinCovariation.getProteinB().contains(newProteinB)) {
                    addCovariationIfProteinsPartOfComplex(pairs, proteinA, newProteinB, proteinCovariation.getProbability());
                }
            }
        }

        return pairs;
    }

    private void addCovariationIfProteinsPartOfComplex(
            List<ProteinPairCovariation> pairs,
            String proteinA,
            String proteinB,
            Double probability) {

        if (isProteinPairPartOfComplex(proteinA, proteinB)) {
            pairs.add(new ProteinPairCovariation(proteinA, proteinB, probability));
        }
    }

    private boolean isProteinPairPartOfComplex(String proteinA, String proteinB) {
        if (proteinsInIntact.contains(proteinA)) {
            if (proteinsInIntact.contains(proteinB)) {
                for (String complexId : complexesInIntact.keySet()) {
                    Set<String> participants = complexesInIntact.get(complexId);
                    if (participants.stream().anyMatch(p -> p.equals(proteinA))) {
                        if (participants.stream().anyMatch(p -> p.equals(proteinB))) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}

package uk.ac.ebi.complex.service.covariation.processor;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.batch.processor.AbstractBatchProcessor;
import uk.ac.ebi.complex.service.covariation.model.ProteinCovariation;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class ProteinCovariationPartitionProcessor extends AbstractBatchProcessor<ProteinCovariation, List<ProteinPairCovariation>> {

    private final int partitionIndex;

    private Map<String, String> uniprotProteinMapping;
    private Map<String, Set<String>> proteinsInIntact;
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

        proteinsInIntact = new HashMap<>();
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
                proteinsInIntact.put(row[0], Set.of(row[1].split(";")));
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
        Set<String> proteinsA = new HashSet<>(proteinCovariation.getProteinA());
        for (String proteinA : proteinCovariation.getProteinA()) {
            if (uniprotProteinMapping.containsKey(proteinA)) {
                proteinsA.add(uniprotProteinMapping.get(proteinA));
            }
        }

        Set<String> proteinsB = new HashSet<>(proteinCovariation.getProteinB());
        for (String proteinB : proteinCovariation.getProteinB()) {
            if (uniprotProteinMapping.containsKey(proteinB)) {
                proteinsB.add(uniprotProteinMapping.get(proteinB));
            }
        }

        return addCovariationIfProteinsPartOfComplex(proteinsA, proteinsB, proteinCovariation.getProbability());
    }

    private List<ProteinPairCovariation> addCovariationIfProteinsPartOfComplex(
            Collection<String> proteinsA,
            Collection<String> proteinsB,
            Double probability) {

        List<ProteinPairCovariation> pairs = new ArrayList<>();

        Set<String> proteinAcsA = proteinsA.stream()
                .filter(proteinsInIntact::containsKey)
                .map(proteinsInIntact::get)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        Set<String> proteinAcsB = proteinsB.stream()
                .filter(proteinsInIntact::containsKey)
                .map(proteinsInIntact::get)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        if (isProteinPairPartOfComplex(proteinAcsA, proteinAcsB)) {
            for (String proteinAcA : proteinAcsA) {
                for (String proteinAcB : proteinAcsB) {
                    pairs.add(new ProteinPairCovariation(proteinAcA, proteinAcB, probability));
                }
            }
        }

        return pairs;
    }

    private boolean isProteinPairPartOfComplex(Set<String> proteinAcsA, Set<String> proteinAcsB) {
        for (String complexId : complexesInIntact.keySet()) {
            Set<String> participants = complexesInIntact.get(complexId);
            for (String participantA : participants) {
                if (proteinAcsA.contains(participantA)) {
                    for (String participantB : participants) {
                        if (proteinAcsB.contains(participantB)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}

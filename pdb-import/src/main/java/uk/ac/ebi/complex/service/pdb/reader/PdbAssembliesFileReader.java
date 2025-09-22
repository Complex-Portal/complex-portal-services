package uk.ac.ebi.complex.service.pdb.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.AnnotationUtils;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.pdb.model.AssemblyEntry;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesFileReader {

    private static final String DEFAULT_PROTEIN_ID_REGEX = "(([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})(-[0-9]+)?(-PRO_[0-9]{10})?)";

    private final FileConfiguration fileConfiguration;
    private final IntactDao intactDao;

    public Set<AssemblyEntry> readAssembliesFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();

        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        String uniProtIdRegex = getUniprotRegex();
        Set<AssemblyEntry> assemblyEntries = new HashSet<>();
        csvReader.forEach(csvLine -> {
            if (csvLine.length > 1 || (csvLine.length == 1 && !csvLine[0].isEmpty())) {
                List<String> complexIds = StringUtils.isEmpty(csvLine[4]) ? List.of() : List.of(csvLine[4]);
                List<String> assemblies = List.of(csvLine[7].split("_")[0]);
                Set<String> proteinIds = getProteinIds(uniProtIdRegex, csvLine[1].split(","));
                assemblyEntries.add(AssemblyEntry.builder()
                        .assemblies(assemblies)
                        .complexIds(complexIds)
                        .proteins(proteinIds.stream().map(proteinId -> UniprotProtein.builder().proteinAc(proteinId).build()).collect(Collectors.toSet()))
                        .build());
            }
        });
        csvReader.close();

        return assemblyEntries;
    }

    public Set<AssemblyEntry> readAssembliesFromParsedFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();

        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        Set<AssemblyEntry> assemblyEntries = new HashSet<>();
        csvReader.forEach(csvLine -> {
            List<String> assemblies = Stream.of(csvLine[0].split(" ")).collect(Collectors.toList());
            List<String> complexIds = StringUtils.isEmpty(csvLine[1])
                    ? List.of()
                    : Stream.of(csvLine[1].split(" ")).collect(Collectors.toList());
            if (!StringUtils.isEmpty(csvLine[2])) {
                Set<UniprotProtein> proteins = Stream.of(csvLine[2].split(" ")).map(this::proteinFromString).collect(Collectors.toSet());
                assemblyEntries.add(AssemblyEntry.builder()
                        .assemblies(assemblies)
                        .complexIds(complexIds)
                        .proteins(proteins)
                        .build());
            }
        });
        csvReader.close();

        return assemblyEntries;
    }

    private String getUniprotRegex() {
        IntactCvTerm uniprotCvTerm = intactDao.getCvTermDao().getByUniqueIdentifier(Xref.UNIPROTKB_MI, IntactUtils.DATABASE_OBJCLASS);
        if (uniprotCvTerm != null) {
            Annotation regexAnnotation = AnnotationUtils.collectFirstAnnotationWithTopic(
                    uniprotCvTerm.getAnnotations(),
                    Annotation.VALIDATION_REGEXP_MI,
                    Annotation.VALIDATION_REGEXP);
            if (regexAnnotation != null) {
                return regexAnnotation.getValue();
            }
        }
        return DEFAULT_PROTEIN_ID_REGEX;
    }

    private Set<String> getProteinIds(String uniProtIdRegex, String[] proteins) {
        Set<String> parsedProteinIds = new HashSet<>();
        for (String protein : proteins) {
            if (protein.contains("::")) {
                parsedProteinIds.addAll(getProteinIds(uniProtIdRegex, protein.split("::")));
            } else {
                String proteinId = getProteinId(uniProtIdRegex, protein);
                if (proteinId != null) {
                    parsedProteinIds.add(proteinId);
                }
            }
        }
        return parsedProteinIds;
    }

    private String getProteinId(String uniProtIdRegex, String protein) {
        String id = protein.split("_")[0];
        if (id.matches(uniProtIdRegex)) {
            return id;
        }
        return null;
    }

    private UniprotProtein proteinFromString(String proteinString) {
        String[] proteinFields = proteinString.split("\\|");
        return UniprotProtein.builder()
                .proteinAc(proteinFields[0])
                .organism(proteinFields.length > 1 ? parseOrganism(proteinFields[1]) : null)
                .build();
    }

    private Integer parseOrganism(String organism) {
        try {
            return Integer.valueOf(organism);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

package uk.ac.ebi.complex.service.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.AllArgsConstructor;
import lombok.Data;
import uk.ac.ebi.complex.service.config.ComplexServiceConfiguration;
import uk.ac.ebi.complex.service.model.UniplexCluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;


@Data
@AllArgsConstructor
public class UniplexClusterWriter {

    private ComplexServiceConfiguration config;

    public void writeClustersToFile(Collection<UniplexCluster> clusters) throws IOException {
        File outputFile = config.outputPath().toFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(config.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (config.isHeader()) {
            csvWriter.writeNext(new String[]{"cluster_ids", "cluster_confidence", "uniprot_acs"});
        }
        clusters.forEach(cluster -> csvWriter.writeNext(clusterToStringArray(cluster)));
        csvWriter.close();
    }

    private String[] clusterToStringArray(UniplexCluster cluster) {
        String clusterIds = String.join(" ", cluster.getClusterIds());
        String clusterConfidence = cluster.getClusterConfidence().toString();
        String uniprotAcs = String.join(" ", cluster.getUniprotAcs());
        return new String[]{clusterIds, clusterConfidence, uniprotAcs};
    }
}

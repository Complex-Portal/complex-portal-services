package uk.ac.ebi.complex.service.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.AllArgsConstructor;
import uk.ac.ebi.complex.service.model.UniplexCluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

@AllArgsConstructor
public class UniplexClusterWriter {

    private final String separator;
    private final boolean header;

    public void writeClustersToFile(Collection<UniplexCluster> clusters, String outputFileName) throws IOException {
        File outputFile = new File(outputFileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator.charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (header) {
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

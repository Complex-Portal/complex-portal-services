package uk.ac.ebi.complex.service.covariation.partitioner;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

@Log4j
@Component
@StepScope
@RequiredArgsConstructor
public class ProteinCovariationPartitioner implements Partitioner {

    private final FileConfiguration fileConfiguration;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        log.info("Start Partition");
        Map<String, ExecutionContext> partitionMap = new HashMap<>();

        long numberOfLines = 0;
        try {
            numberOfLines = Files.lines(new File(fileConfiguration.getInputFileName()).toPath()).count();
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        int partitionIndex = 0;
        int partitionSize = (int) (numberOfLines / gridSize);
        int startLine = 0;

        while (startLine <= numberOfLines) {
            ExecutionContext ctxMap = new ExecutionContext();
            ctxMap.putInt("startLine", startLine);
            ctxMap.putInt("partitionSize", partitionSize);
            ctxMap.putInt("partitionIndex", partitionIndex);
            partitionIndex++;
            partitionMap.put("Thread:LSS:" + (partitionIndex), ctxMap);

            startLine = startLine + partitionSize;
        }

        log.info("Number of partitions created: " + partitionMap.size());
        return partitionMap;
    }
}

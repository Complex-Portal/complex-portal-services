package uk.ac.ebi.complex.service.pdb.processor;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class PdbFileProcessorTasklet implements Tasklet {

    private final PdbFileProcessor pdbFileProcessor;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        pdbFileProcessor.processFile();
        return RepeatStatus.FINISHED;
    }
}

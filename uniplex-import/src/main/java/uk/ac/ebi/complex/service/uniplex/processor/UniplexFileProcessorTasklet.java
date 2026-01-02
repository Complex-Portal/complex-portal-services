package uk.ac.ebi.complex.service.uniplex.processor;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class UniplexFileProcessorTasklet implements Tasklet {

    private final UniplexFileProcessor uniplexFileProcessor;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        uniplexFileProcessor.processFile();
        return RepeatStatus.FINISHED;
    }
}

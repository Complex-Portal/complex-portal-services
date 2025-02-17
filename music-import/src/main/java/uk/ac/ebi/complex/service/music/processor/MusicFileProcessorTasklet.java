package uk.ac.ebi.complex.service.music.processor;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class MusicFileProcessorTasklet implements Tasklet {

    private final MusicFileProcessor musicFileProcessor;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        musicFileProcessor.processFile();
        return RepeatStatus.FINISHED;
    }
}

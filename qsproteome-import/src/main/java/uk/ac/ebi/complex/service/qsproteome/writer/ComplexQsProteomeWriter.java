package uk.ac.ebi.complex.service.qsproteome.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.batch.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.qsproteome.model.ComplexWithProteomeStructures;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@SuperBuilder
public class ComplexQsProteomeWriter extends AbstractBatchWriter<ComplexWithProteomeStructures, Complex> {

    private final IntactDao intactDao;
    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();

    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        try {
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        try {
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    public void write(List<? extends ComplexWithProteomeStructures> items) throws Exception {
        if (!appProperties.isDryRunMode()) {
            Map<String, IntactComplex> complexesToSave = new HashMap<>();
            for (ComplexWithProteomeStructures item: items) {
                IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
                try {

                } catch (Exception e) {
                    log.error("Error writing to DB complex xrefs for complex id: " + item.getComplexId(), e);
                    errorReportWriter.write(List.of(item.getComplexId()), e.getMessage());
                }
            }

            this.intactService.saveOrUpdate(complexesToSave.values());
        }
    }

    private void removeXref(Collection<Xref> complexXrefs, InteractorXref xrefToRemove) {
        complexXrefs.remove(xrefToRemove);
    }

    private void updateXrefQualifier(Collection<Xref> complexXrefs, InteractorXref xrefToUpdate) throws CvTermNotFoundException {
        for (Xref xref : complexXrefs) {
            if (xref instanceof InteractorXref) {
                InteractorXref interactorXref = (InteractorXref) xref;
                if (xrefToUpdate.getAc().equals(interactorXref.getAc())) {
                    // TODO: review qualifier
                    IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
                    interactorXref.setQualifier(qualifier);
                }
            }
        }
    }

    private void addNewXref(IntactComplex complex, String xrefToAdd) throws CvTermNotFoundException {
        // TODO: review database and qualifier
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, ComplexManager.WWPDB_DB_MI);
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
        InteractorXref newXref = new InteractorXref(database, xrefToAdd, qualifier);
        complex.getIdentifiers().add(newXref);
    }

    private IntactCvTerm findCvTerm(String clazz, String id) throws CvTermNotFoundException {
        String key = clazz + "_" + id;
        if (cvTermMap.containsKey(key)) {
            return cvTermMap.get(key);
        }
        IntactCvTerm cvTerm = intactDao.getCvTermDao().getByUniqueIdentifier(id, clazz);
        if (cvTerm != null) {
            cvTermMap.put(key, cvTerm);
            return cvTerm;
        }
        throw new CvTermNotFoundException("CV Term not found with class '" + clazz + "' and id '" + id + "'");
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }
}

package uk.ac.ebi.complex.service.qsproteome.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.batch.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.qsproteome.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.qsproteome.model.ComplexWithProteomeStructures;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.AbstractIntactXref;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.io.File;
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
public class ComplexQsProteomeWriter extends AbstractBatchWriter<ComplexWithProteomeStructures, Complex> {

    private static final String DATABASE = "qsproteome";
    private static final String DATABASE_MI = "MI:2443";

    private final IntactDao intactDao;
    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();

    private ProcessReportWriter noChangesReportWriter;
    private ProcessReportWriter xrefsToAddReportWriter;
    private ProcessReportWriter xrefsToRemoveReportWriter;
    private ProcessReportWriter xrefsToUpdateReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        try {
            this.noChangesReportWriter.flush();
            this.xrefsToAddReportWriter.flush();
            this.xrefsToRemoveReportWriter.flush();
            this.xrefsToUpdateReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        try {
            this.noChangesReportWriter.close();
            this.xrefsToAddReportWriter.close();
            this.xrefsToRemoveReportWriter.close();
            this.xrefsToUpdateReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    public void write(List<? extends ComplexWithProteomeStructures> items) throws Exception {
        List<IntactComplex> complexesToSave = new ArrayList<>();
        for (ComplexWithProteomeStructures item: items) {
            try {
                IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
                String complexId = complex.getComplexAc();

                List<String> xrefsToAdd = new ArrayList<>();
                List<InteractorXref> xrefsToRemove = new ArrayList<>();
                List<InteractorXref> xrefsToUpdate = new ArrayList<>();

                checkComplexXrefs(complex, item.getQsProteomeIds(), xrefsToAdd, xrefsToRemove, xrefsToUpdate);

                if (xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToUpdate.isEmpty()) {
                    if (!item.getQsProteomeIds().isEmpty()) {
                        noChangesReportWriter.write(complexId, complex.isPredictedComplex(), item.getQsProteomeIds());
                    }
                } else {
                    if (!xrefsToAdd.isEmpty()) {
                        xrefsToAddReportWriter.write(complexId, complex.isPredictedComplex(), xrefsToAdd);
                    }
                    if (!xrefsToRemove.isEmpty()) {
                        xrefsToRemoveReportWriter.write(
                                complexId,
                                complex.isPredictedComplex(),
                                xrefsToRemove.stream().map(AbstractIntactXref::getId).collect(Collectors.toList()));
                    }
                    if (!xrefsToUpdate.isEmpty()) {
                        xrefsToUpdateReportWriter.write(
                                complexId,
                                complex.isPredictedComplex(),
                                xrefsToRemove.stream().map(AbstractIntactXref::getId).collect(Collectors.toList()));
                    }
                    if (!appProperties.isDryRunMode()) {
                        if (!xrefsToAdd.isEmpty()) {
                            addNewXrefs(complex, xrefsToAdd);
                        }
                        if (!xrefsToRemove.isEmpty()) {
                            removeXrefs(complex, xrefsToRemove);
                        }
                        if (!xrefsToUpdate.isEmpty()) {
                            updateXrefs(complex, xrefsToUpdate);
                        }
                        complexesToSave.add(complex);
                    }
                }
            } catch (Exception e) {
                log.error("Error writing to DB complex xrefs for complex id: " + item.getComplexId(), e);
                errorReportWriter.write(List.of(item.getComplexId()), e.getMessage());
            }
        }

//        if (!appProperties.isDryRunMode()) {
//            this.intactService.saveOrUpdate(complexesToSave);
//        }
    }

    private void checkComplexXrefs(
            IntactComplex complex,
            Collection<String> structureIds,
            List<String> xrefsToAdd,
            List<InteractorXref> xrefsToRemove,
            List<InteractorXref> xrefsToUpdate) {

        List<Xref> xrefs = new ArrayList<>();
        xrefs.addAll(XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), DATABASE_MI, DATABASE));
        xrefs.addAll(XrefUtils.collectAllXrefsHavingDatabase(complex.getXrefs(), DATABASE_MI, DATABASE));

        Set<String> matchesFound = new HashSet<>();

        for (String structureId: structureIds) {
            if (checkIStructureMatchesAnyXref(structureId, xrefs, xrefsToUpdate, matchesFound)) {
                matchesFound.add(structureId.toLowerCase());
            } else {
                xrefsToAdd.add(structureId);
            }
        }

        for (Xref xref: xrefs) {
            InteractorXref interactorXref = (InteractorXref) xref;
            if (!matchesFound.contains(interactorXref.getId().toLowerCase())) {
                xrefsToRemove.add(interactorXref);
            }
        }
    }

    private boolean checkIStructureMatchesAnyXref(
            String structureId,
            List<Xref> xrefs,
            List<InteractorXref> xrefsToUpdate,
            Set<String> matchesFound) {

        for (Xref xref: xrefs) {
            InteractorXref interactorXref = (InteractorXref) xref;
            if (interactorXref.getId().equalsIgnoreCase(structureId)) {
                matchesFound.add(structureId);
                if (!Xref.IDENTITY_MI.equals(interactorXref.getQualifier().getMIIdentifier())) {
                    xrefsToUpdate.add(interactorXref);
                }
                return true;
            }
        }
        return false;
    }

    private void addNewXrefs(IntactComplex complex, Collection<String> xrefsToAdd) throws CvTermNotFoundException {
        for (String xrefToAdd: xrefsToAdd) {
            IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, DATABASE_MI);
            IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
            InteractorXref newXref = new InteractorXref(database, xrefToAdd, qualifier);
            complex.getIdentifiers().add(newXref);
        }
    }

    private void removeXrefs(IntactComplex complex, Collection<InteractorXref> xrefsToRemove) {
        for (InteractorXref xrefToRemove: xrefsToRemove) {
            complex.getIdentifiers().remove(xrefToRemove);
            complex.getXrefs().remove(xrefToRemove);
        }
    }

    private void updateXrefs(IntactComplex complex, Collection<InteractorXref> xrefsToUpdate) throws CvTermNotFoundException {
        for (InteractorXref xrefToUpdate: xrefsToUpdate) {
            updateXrefQualifier(complex.getIdentifiers(), xrefToUpdate);
            updateXrefQualifier(complex.getXrefs(), xrefToUpdate);
        }
    }

    private void updateXrefQualifier(Collection<Xref> complexXrefs, InteractorXref xrefToUpdate) throws CvTermNotFoundException {
        for (Xref xref : complexXrefs) {
            if (xref instanceof InteractorXref) {
                InteractorXref interactorXref = (InteractorXref) xref;
                if (xrefToUpdate.getAc().equals(interactorXref.getAc())) {
                    IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, QUALIFIER_MI);
                    interactorXref.setQualifier(qualifier);
                }
            }
        }
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

        String xrefsToAddFile = this.appProperties.isDryRunMode() ? "xrefs_to_add" : "xrefs_added";
        String xrefsToRemoveFile = this.appProperties.isDryRunMode() ? "xrefs_to_remove" : "xrefs_removed";
        String xrefsToUpdateFile = this.appProperties.isDryRunMode() ? "xrefs_to_update" : "xrefs_updated";

        this.noChangesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "no_changes" + extension), separator, header, ProcessReportWriter.COMPLEXES_WITH_QS_PROTEOME_STRUCTURES, false);
        this.xrefsToAddReportWriter = new ProcessReportWriter(
                new File(reportDirectory, xrefsToAddFile + extension), separator, header, ProcessReportWriter.COMPLEXES_WITH_QS_PROTEOME_STRUCTURES, false);
        this.xrefsToRemoveReportWriter = new ProcessReportWriter(
                new File(reportDirectory, xrefsToRemoveFile + extension), separator, header, ProcessReportWriter.COMPLEXES_WITH_QS_PROTEOME_STRUCTURES, false);
        this.xrefsToUpdateReportWriter = new ProcessReportWriter(
                new File(reportDirectory, xrefsToUpdateFile + extension), separator, header, ProcessReportWriter.COMPLEXES_WITH_QS_PROTEOME_STRUCTURES, false);
        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }
}

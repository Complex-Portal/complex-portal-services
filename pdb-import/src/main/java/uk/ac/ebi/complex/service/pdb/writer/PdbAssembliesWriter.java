package uk.ac.ebi.complex.service.pdb.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.batch.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.pdb.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.pdb.model.ComplexWithAssemblyXrefs;
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
public class PdbAssembliesWriter extends AbstractBatchWriter<ComplexWithAssemblyXrefs, Complex> {

    private static final String WWPDB_DB_MI = "MI:0805";
    private static final String WWPDB_DB_NAME = "wwpdb";

    private static final String ML_ECO_CODE = "ECO:0008004";
    private static final String COMP_EVIDENCE_ECO_CODE = "ECO:0007653";

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
    public void write(List<? extends ComplexWithAssemblyXrefs> items) throws Exception {
        if (!appProperties.isDryRunMode()) {
            Map<String, IntactComplex> complexesToSave = new HashMap<>();
            for (ComplexWithAssemblyXrefs item: items) {
                IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
                try {
                    for (InteractorXref xrefToRemove: item.getXrefsToRemove()) {
                        removeXref(complex.getIdentifiers(), xrefToRemove);
                        removeXref(complex.getXrefs(), xrefToRemove);
                    }
                    for (InteractorXref xrefToUpdate: item.getXrefsToUpdate()) {
                        updateXrefQualifier(complex.getIdentifiers(), xrefToUpdate);
                        updateXrefQualifier(complex.getXrefs(), xrefToUpdate);
                    }
                    for (String xrefToAdd: item.getXrefsToAdd()) {
                        addNewXref(complex, xrefToAdd);
                    }

                    if (complex.isPredictedComplex()) {
                        Collection<Xref> pdbXrefs = XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME);
                        if (pdbXrefs.isEmpty()) {
                            if (!complex.getEvidenceType().getMIIdentifier().equals(ML_ECO_CODE)) {
                                IntactCvTerm newEvidenceType = findCvTerm(IntactUtils.DATABASE_OBJCLASS, ML_ECO_CODE);
                                complex.setEvidenceType(newEvidenceType);
                            }
                        } else {
                            if (!complex.getEvidenceType().getMIIdentifier().equals(COMP_EVIDENCE_ECO_CODE)) {
                                IntactCvTerm newEvidenceType = findCvTerm(IntactUtils.DATABASE_OBJCLASS, COMP_EVIDENCE_ECO_CODE);
                                complex.setEvidenceType(newEvidenceType);
                            }
                        }
                    }

                } catch (Exception e) {
                    log.error("Error writing to DB complex xrefs for complex id: " + item.getComplexId(), e);
                    errorReportWriter.write(item.getComplexId(), e.getMessage());
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
                    IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
                    interactorXref.setQualifier(qualifier);
                }
            }
        }
    }

    private void addNewXref(IntactComplex complex, String xrefToAdd) throws CvTermNotFoundException {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, WWPDB_DB_MI);
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

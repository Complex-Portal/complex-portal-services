package uk.ac.ebi.complex.service.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.config.AppProperties;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblyXrefs;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;
import uk.ac.ebi.intact.jami.service.ComplexService;
import uk.ac.ebi.intact.jami.synchronizer.listener.impl.DbSynchronizerStatisticsReporter;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesWriter implements ItemWriter<ComplexWithAssemblyXrefs>, ItemStream {

    private static final String WWPDB_DB_MI = "MI:0805";

    public final static String PERSIST_MAP_COUNT = "persisted.map";
    public final static String MERGE_MAP_COUNT = "merged.map";
    public final static String DELETED_MAP_COUNT = "deleted.map";
    public final static String MERGED_TRANSIENT_MAP_COUNT = "merged.transient.map";
    public final static String REPLACED_TRANSIENT_MAP_COUNT = "transient.replaced.map";

    private final IntactDao intactDao;
    private final ComplexService complexService;
    private final FileConfiguration fileConfiguration;
    private final AppProperties appProperties;

    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();

    private DbSynchronizerStatisticsReporter synchronizerListener;

    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
//        Assert.notNull(executionContext, "ExecutionContext must not be null");
//        try {
//            initialiseReportWriters();
//        } catch (IOException e) {
//            throw new ItemStreamException("Report file  writer could not be opened", e);
//        }
//
//        this.synchronizerListener = new DbSynchronizerStatisticsReporter();
//
//        // restore previous statistics
//        if (!executionContext.isEmpty()){
//            for (Map.Entry<String, Object> entry : executionContext.entrySet()){
//                if (entry.getKey().startsWith(PERSIST_MAP_COUNT+"_")){
//                    try {
//                        this.synchronizerListener.getPersistedCounts()
//                                .put(Class.forName(entry.getKey().substring(PERSIST_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
//                    } catch (ClassNotFoundException e) {
//                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
//                    }
//                }
//                else if (entry.getKey().startsWith(MERGE_MAP_COUNT+"_")){
//                    try {
//                        this.synchronizerListener.getMergedCounts()
//                                .put(Class.forName(entry.getKey().substring(MERGE_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
//                    } catch (ClassNotFoundException e) {
//                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
//                    }
//                }
//                else if (entry.getKey().startsWith(DELETED_MAP_COUNT+"_")){
//                    try {
//                        this.synchronizerListener.getDeletedCounts()
//                                .put(Class.forName(entry.getKey().substring(DELETED_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
//                    } catch (ClassNotFoundException e) {
//                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
//                    }
//                }
//                else if (entry.getKey().startsWith(MERGED_TRANSIENT_MAP_COUNT+"_")){
//                    try {
//                        this.synchronizerListener.getMergedTransientCounts()
//                                .put(Class.forName(entry.getKey().substring(MERGED_TRANSIENT_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
//                    } catch (ClassNotFoundException e) {
//                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
//                    }
//                }
//                else if (entry.getKey().startsWith(REPLACED_TRANSIENT_MAP_COUNT+"_")){
//                    try {
//                        this.synchronizerListener.getTransientReplacedCounts()
//                                .put(Class.forName(entry.getKey().substring(REPLACED_TRANSIENT_MAP_COUNT.length() + 1)), (Integer) entry.getValue());
//                    } catch (ClassNotFoundException e) {
//                        throw new ItemStreamException("Cannot reload persisted statistics from execution context",e);
//                    }
//                }
//            }
//        }
//
//        this.complexService.getIntactDao().getSynchronizerContext().initialiseDbSynchronizerListener(synchronizerListener);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
//        Assert.notNull(executionContext, "ExecutionContext must not be null");
//        try {
//            this.errorReportWriter.flush();
//        } catch (IOException e) {
//            throw new ItemStreamException("Report file writer could not be flushed", e);
//        }
//
//        // persist statistics
//        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getPersistedCounts().entrySet()){
//            executionContext.put(PERSIST_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
//        }
//        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getMergedCounts().entrySet()){
//            executionContext.put(MERGE_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
//        }
//        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getTransientReplacedCounts().entrySet()){
//            executionContext.put(REPLACED_TRANSIENT_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
//        }
//        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getMergedTransientCounts().entrySet()){
//            executionContext.put(MERGED_TRANSIENT_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
//        }
//        for (Map.Entry<Class,Integer> entry : this.synchronizerListener.getDeletedCounts().entrySet()){
//            executionContext.put(DELETED_MAP_COUNT+"_"+entry.getKey().getCanonicalName(), entry.getValue());
//        }
    }

    @Override
    public void close() throws ItemStreamException {
//        try {
//            this.errorReportWriter.close();
//        } catch (IOException e) {
//            throw new ItemStreamException("Report file writer could not be closed", e);
//        }
//
//        // flush statistics
//        log.info("Created objects in database: ");
//        for (Map.Entry<Class, Integer> entry : synchronizerListener.getPersistedCounts().entrySet()){
//            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
//        }
//        log.info("Objects already in the database: ");
//        for (Map.Entry<Class, Integer> entry : synchronizerListener.getMergedTransientCounts().entrySet()){
//            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
//        }
//        log.info("Objects replaced with existing instance in the database: ");
//        for (Map.Entry<Class, Integer> entry : synchronizerListener.getTransientReplacedCounts().entrySet()){
//            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
//        }
//        log.info("Existing Objects merged: ");
//        for (Map.Entry<Class, Integer> entry : synchronizerListener.getMergedCounts().entrySet()){
//            log.info(entry.getKey().getSimpleName()+": "+entry.getValue());
//        }
//        // remove listener
//        this.synchronizerListener = null;
    }

    @Override
    public void write(List<? extends ComplexWithAssemblyXrefs> items) throws Exception {
//        Map<String, IntactComplex> complexesToSave = new HashMap<>();
//        for (ComplexWithAssemblyXrefs item: items) {
//            IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
//            try {
//                for (InteractorXref xrefToRemove: item.getXrefsToRemove()) {
//                    removeXref(complex.getIdentifiers(), xrefToRemove);
//                    removeXref(complex.getXrefs(), xrefToRemove);
//                }
//                for (InteractorXref xrefToUpdate: item.getXrefsToUpdate()) {
//                    updateXrefQualifier(complex.getIdentifiers(), xrefToUpdate);
//                    updateXrefQualifier(complex.getXrefs(), xrefToUpdate);
//                }
//                for (String xrefToAdd: item.getXrefsToAdd()) {
//                    addNewXref(complex, xrefToAdd);
//                }
//            } catch (Exception e) {
//                log.error("Error writing to DB complex xrefs for complex id: " + item.getComplexId(), e);
//                errorReportWriter.write(item.getComplexId(), e.getMessage());
//            }
//        }
//
//        if (!appProperties.isDryRunMode()) {
//            this.complexService.saveOrUpdate(complexesToSave.values());
//        }
    }

    private void removeXref(Collection<Xref> complexXrefs, InteractorXref xrefToRemove) {
        for (Xref xref: complexXrefs) {
            InteractorXref interactorXref = (InteractorXref) xref;
            if (xrefToRemove.getAc().equals(interactorXref.getAc())) {
                complexXrefs.remove(xref);
            }
        }
    }

    private void updateXrefQualifier(Collection<Xref> complexXrefs, InteractorXref xrefToUpdate) throws CvTermNotFoundException {
        for (Xref xref: complexXrefs) {
            InteractorXref interactorXref = (InteractorXref) xref;
            if (xrefToUpdate.getAc().equals(interactorXref.getAc())) {
                IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
                interactorXref.setQualifier(qualifier);
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

    private void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }
}

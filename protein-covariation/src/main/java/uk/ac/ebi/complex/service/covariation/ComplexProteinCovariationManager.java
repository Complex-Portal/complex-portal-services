package uk.ac.ebi.complex.service.covariation;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import uk.ac.ebi.complex.service.covariation.model.ComplexProteinCovariation;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProteinPairCovariation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@Component
@AllArgsConstructor
public class ComplexProteinCovariationManager {

    private final IntactDao intactDao;

    public ComplexProteinCovariation getProteinCovariations(IntactComplex complex) {
        Collection<ModelledComparableParticipant> complexProteins = getProteinComponents(complex);

        List<String> complexProteinIds = complexProteins.stream()
                .map(ModelledComparableParticipant::getInteractorId)
                .sorted()
                .collect(Collectors.toList());
        Set<String> complexProteinIdsSet = new HashSet<>(complexProteinIds);

        Collection<IntactProteinPairCovariation> proteinPairCovariations = readProteinCovariationsFromDb(complexProteinIdsSet);

        List<List<Double>> proteinCovariationsMatrix = new ArrayList<>();
        int numberOfPairsWithCovariationFound = 0;
        int totalNumberOfPairs = 0;

        for (int i = 0; i < complexProteinIds.size(); i++) {
            List<Double> proteinCovariationsRow = new ArrayList<>();

            // First, fill out previously filled values mirrored on the other side of the matrix
            for (int j = 0; j < i; j++) {
                proteinCovariationsRow.add(proteinCovariationsMatrix.get(j).get(i));
            }

            // Then, set covariation diagonal to 1.0
            proteinCovariationsRow.add(1.0);

            for (int j = i + 1; j < complexProteinIds.size(); j++) {
                totalNumberOfPairs++;
                Double probability = findProbability(proteinPairCovariations, complexProteinIds.get(i), complexProteinIds.get(j));

                if (probability != null) {
                    proteinCovariationsRow.add(probability);
                    numberOfPairsWithCovariationFound++;
                } else {
                    proteinCovariationsRow.add(0.0);
                }
            }

            proteinCovariationsMatrix.add(proteinCovariationsRow);
        }

        ComplexProteinCovariation complexProteinCovariation = new ComplexProteinCovariation();
        complexProteinCovariation.setProteinIds(complexProteinIds);
        complexProteinCovariation.setCovariationCoverage((double) numberOfPairsWithCovariationFound / totalNumberOfPairs);
        complexProteinCovariation.setProteinCovariations(proteinCovariationsMatrix);

        return complexProteinCovariation;
    }

    private Collection<ModelledComparableParticipant> getProteinComponents(IntactComplex complex) {
        return complex.getComparableParticipants(
                true,
                proteinAc -> intactDao.getProteinDao().getByAc(proteinAc));
    }

    private Collection<IntactProteinPairCovariation> readProteinCovariationsFromDb(Set<String> complexProteinIds) {
        return intactDao.getIntactProteinPairCovariationDao().getAllForProteinIds(complexProteinIds);
    }

    private Double findProbability(Collection<IntactProteinPairCovariation> proteinPairCovariations, String proteinA, String proteinB) {
        for (IntactProteinPairCovariation proteinPairCovariation : proteinPairCovariations) {
            if (proteinPairCovariation.getProteinA().equals(proteinA) && proteinPairCovariation.getProteinB().equals(proteinB)) {
                return proteinPairCovariation.getProbability();
            }
            if (proteinPairCovariation.getProteinA().equals(proteinB) && proteinPairCovariation.getProteinB().equals(proteinA)) {
                return proteinPairCovariation.getProbability();
            }
        }
        return null;
    }
}

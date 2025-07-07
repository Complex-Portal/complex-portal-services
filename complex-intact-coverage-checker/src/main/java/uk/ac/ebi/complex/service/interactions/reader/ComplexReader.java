package uk.ac.ebi.complex.service.interactions.reader;

import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.database.JpaPagingItemReader;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

@Log4j
public class ComplexReader extends JpaPagingItemReader<IntactComplex> {

    private final String taxId;

    public ComplexReader(String taxId) {
        super();
        this.taxId = taxId;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        String query = "select i " +
                "from IntactComplex i " +
                "join i.organism as o " +
                "where o.dbTaxid = '" + taxId + "' " +
                "and i.predictedComplex is true " +
                "order by i.ac";
        setQueryString(query);

        super.afterPropertiesSet();
    }
}

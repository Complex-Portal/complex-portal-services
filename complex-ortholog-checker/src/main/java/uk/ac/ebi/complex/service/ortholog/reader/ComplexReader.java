/**
 * Copyright 2009 The European Bioinformatics Institute, and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.complex.service.ortholog.reader;

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
                "order by i.ac";
        setQueryString(query);

        super.afterPropertiesSet();
    }

    @Override
    protected IntactComplex doRead() throws Exception {
        IntactComplex result = super.doRead();
        if (result != null) {
            log.info("-- DEBUG (read) -- " + result.getAc() + " - " + result.getComplexAc());
        }
        return result;
    }
}

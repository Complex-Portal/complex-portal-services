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

import org.springframework.batch.item.database.JpaPagingItemReader;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

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
                "where o.dbTaxid = '" + taxId + "'";
        setQueryString(query);

        super.afterPropertiesSet();
    }
}

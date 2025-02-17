package uk.ac.ebi.complex.service.batch.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

public class UniProt {
    public static final String UNIPROT_API = "https://rest.uniprot.org";

    public static class IdMapping {
        public static final String ID_MAPPING_URL = UNIPROT_API + "/idmapping";

        public static class Submit {
            public static String SUBMIT_URL = ID_MAPPING_URL + "/run";

            @Jacksonized
            @Builder
            @Data
            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class Result {
                private String jobId;
            }
        }


        public static class Poll {
            public static String POLL_URL = ID_MAPPING_URL + "/status/";

            @Jacksonized
            @Builder
            @Data
            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class Result {
                private Status jobStatus;

                public enum Status {
                    RUNNING,
                    FINISHED,
                    QUEUED,
                    NEW
                }
            }
        }


        public static class Result {
            public static String RESULT_URL = ID_MAPPING_URL + "/uniprotkb/results/stream/";
        }
    }
}

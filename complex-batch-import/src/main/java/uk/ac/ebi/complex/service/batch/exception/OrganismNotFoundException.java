package uk.ac.ebi.complex.service.batch.exception;

public class OrganismNotFoundException extends Exception {

    public OrganismNotFoundException() {
        super();
    }

    public OrganismNotFoundException(String message) {
        super(message);
    }

    public OrganismNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OrganismNotFoundException(Throwable cause) {
        super(cause);
    }
}

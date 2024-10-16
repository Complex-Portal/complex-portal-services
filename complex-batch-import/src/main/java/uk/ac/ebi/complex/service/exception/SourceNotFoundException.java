package uk.ac.ebi.complex.service.exception;

public class SourceNotFoundException extends Exception {

    public SourceNotFoundException() {
        super();
    }

    public SourceNotFoundException(String message) {
        super(message);
    }

    public SourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SourceNotFoundException(Throwable cause) {
        super(cause);
    }
}

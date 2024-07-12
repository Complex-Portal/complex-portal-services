package uk.ac.ebi.complex.service.exception;

public class ProteinException extends Exception {

    public ProteinException() {
        super();
    }

    public ProteinException(String message) {
        super(message);
    }

    public ProteinException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProteinException(Throwable cause) {
        super(cause);
    }
}

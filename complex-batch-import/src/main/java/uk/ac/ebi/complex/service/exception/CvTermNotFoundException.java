package uk.ac.ebi.complex.service.exception;

public class CvTermNotFoundException extends Exception {

    public CvTermNotFoundException() {
        super();
    }

    public CvTermNotFoundException(String message) {
        super(message);
    }

    public CvTermNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public CvTermNotFoundException(Throwable cause) {
        super(cause);
    }
}

package trident.kafka;

public class FailedFetchException extends RuntimeException {
    public FailedFetchException(Exception e) {
        super(e);
    }
}

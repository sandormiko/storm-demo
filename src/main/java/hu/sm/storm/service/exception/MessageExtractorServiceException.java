package hu.sm.storm.service.exception;

@SuppressWarnings("serial")
public class MessageExtractorServiceException extends Exception {

	public MessageExtractorServiceException(Exception e) {
		super(e);
	}

	public MessageExtractorServiceException(String msg, Exception e) {
		super(msg, e);
	}
}

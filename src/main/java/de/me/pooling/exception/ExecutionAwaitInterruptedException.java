package de.me.pooling.exception;

import java.util.concurrent.RejectedExecutionException;


/**
 * This exception is thrown if thread in blocking execute method was interrupted.
 */
public class ExecutionAwaitInterruptedException extends RejectedExecutionException {

	private static final long serialVersionUID = -4013005926699265066L;

	public ExecutionAwaitInterruptedException() {
	}

	public ExecutionAwaitInterruptedException(String message) {
		super(message);
	}

	public ExecutionAwaitInterruptedException(Throwable cause) {
		super(cause);
	}

	public ExecutionAwaitInterruptedException(String message, Throwable cause) {
		super(message, cause);
	}

}

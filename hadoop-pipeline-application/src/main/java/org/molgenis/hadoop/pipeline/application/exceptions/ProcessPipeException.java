package org.molgenis.hadoop.pipeline.application.exceptions;

public class ProcessPipeException extends RuntimeException
{
	private static final long serialVersionUID = -3951771900588889532L;

	public ProcessPipeException()
	{
		super();
	}

	public ProcessPipeException(String message)
	{
		super(message);
	}

	public ProcessPipeException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public ProcessPipeException(Throwable cause)
	{
		super(cause);
	}

	protected ProcessPipeException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

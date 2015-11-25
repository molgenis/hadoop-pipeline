/**
 * The Javadoc from Oracle's java.lang.RuntimeException was used to create the Javadoc present in this file.
 */

package org.molgenis.hadoop.pipeline.application.exceptions;

import java.io.IOException;

/**
 * An <em>unchecked exception</em> thrown to indicate something went wrong regarding I/O (such as an {@link IOException}
 * ) during the usage of a {@link Sink}.
 */
public class SinkIOException extends SinkException
{
	private static final long serialVersionUID = -3951771900588889532L;

	/**
	 * Constructs a sink IO exception with the specified cause and a detail message of
	 * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the class and detail message of
	 * <tt>cause</tt>).
	 *
	 * @param cause
	 *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A <tt>null</tt>
	 *            value is permitted, and indicates that the cause is nonexistent or unknown.)
	 */
	public SinkIOException(Throwable cause)
	{
		super(cause);
	}
}

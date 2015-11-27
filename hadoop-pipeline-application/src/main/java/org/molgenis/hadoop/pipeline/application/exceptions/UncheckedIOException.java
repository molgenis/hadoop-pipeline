/**
 * The Javadoc from Oracle's java.lang.RuntimeException was used to create the Javadoc present in this file.
 */

package org.molgenis.hadoop.pipeline.application.exceptions;

/**
 * 
 */
public class UncheckedIOException extends RuntimeException
{
	private static final long serialVersionUID = -7594698103814441216L;

	/**
	 * Constructs a new {@link UncheckedIOException} with the specified cause and a detail message of
	 * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the class and detail message of
	 * <tt>cause</tt>). This constructor is useful for runtime exceptions that are little more than wrappers for other
	 * throwables.
	 *
	 * @param cause
	 *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A <tt>null</tt>
	 *            value is permitted, and indicates that the cause is nonexistent or unknown.)
	 */
	public UncheckedIOException(Throwable cause)
	{
		super(cause);
	}
}

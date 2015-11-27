/**
 * The Javadoc from Oracle's java.io.IOException was used to create the Javadoc present in this file.
 */

package org.molgenis.hadoop.pipeline.application.exceptions;

import java.io.IOException;

/**
 * An <em>unchecked exception</em> thrown when an error occurred caused by an invalid bed file (for example, due to not
 * using the correct bed file format as defined in
 * <a href='https://genome.ucsc.edu/FAQ/FAQformat.html#format1'>https://genome.ucsc.edu/FAQ/FAQformat.html#format1</a>.
 */
public class InvalidBedFileException extends IOException
{
	private static final long serialVersionUID = -251713936489606538L;

	/**
	 * Constructs an {@code InvalidBedFileException} with the specified detail message.
	 *
	 * @param message
	 *            The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
	 */
	public InvalidBedFileException(String message)
	{
		super(message);
	}
}

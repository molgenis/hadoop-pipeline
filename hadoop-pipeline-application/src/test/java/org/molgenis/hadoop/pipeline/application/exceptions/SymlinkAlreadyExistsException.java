/**
 * The Javadoc from Oracle's java.nio.file.FileAlreadyExistsException was used to create the Javadoc present in this file.
 */

package org.molgenis.hadoop.pipeline.application.exceptions;

import java.nio.file.FileAlreadyExistsException;

/**
 * An subclass of {@link FileAlreadyExistsException} that is thrown when an attempt to create a Symlink was done but a
 * file/Symlink already exists with that name (at that location).
 */
public class SymlinkAlreadyExistsException extends FileAlreadyExistsException
{
	private static final long serialVersionUID = 3600380167835964386L;

	/**
	 * Constructs an instance of this class.
	 *
	 * @param file
	 *            a string identifying the file or {@code null} if not known
	 */
	public SymlinkAlreadyExistsException(String cause)
	{
		super(cause);
	}
}

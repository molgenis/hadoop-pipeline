package org.molgenis.hadoop.pipeline.application.mapreduce.drivers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.molgenis.hadoop.pipeline.application.exceptions.SymlinkAlreadyExistsException;

/**
 * Manages the Symlinks that are created in the working directory. Is {@code static} so that all created Symlinks in the
 * working directory are stored in a single place. If an attempt is made to create a Symlink with the exact same name
 * (either from the same or a different class), a {@link SymlinkAlreadyExistsException} is thrown to indicate a Symlink
 * with that name was already created using the {@link WorkDirSymlinkManager}. This class is needed when testing a
 * custom subclass of {@link org.apache.hadoop.mapreduce.Mapper} or {@link org.apache.hadoop.mapreduce.Reducer} that
 * retrieves the distributed cache using {@link org.apache.hadoop.mapreduce.JobContext#getCacheArchives()} and/or
 * {@link org.apache.hadoop.mapreduce.JobContext#getCacheFiles()}.
 */
public abstract class WorkDirSymlinkManager
{
	/**
	 * Stores the symlinks that were created (in case a symlink would be created while there is already a file present
	 * with that name in the working directory).
	 */
	private static ArrayList<File> symlinksCreated = new ArrayList<File>();

	/**
	 * Creates a symlink in the working directory to each {@link Path} present in the array. Calls
	 * {@link #createSymlinkInCurrentWorkDirToPath(Path)} for each {@link Path} in {@code tmpFiles}.
	 * 
	 * @param targetFiles
	 *            {@link Path}{@code []}
	 * @throws IOException
	 */
	static void createSymlinkInCurrentWorkDirForEachPath(Path[] targetFiles) throws IOException
	{
		for (int i = 0; i < targetFiles.length; i++)
		{
			createSymlinkInCurrentWorkDirToPath(targetFiles[i]);
		}
	}

	/**
	 * Creates a symlink in the working directory to the {@link Path}. Also adds the created symlink to
	 * {@code symlinksCreated} for when {@link #cleanupDistributedCache()} is called. If a symlink was already created
	 * with that name, a {@link SymlinkAlreadyExistsException} is thrown. If a file already exists with that name (but
	 * wasn't created using this class) or another {@link IOException} would be thrown, the default behavior is used.
	 * 
	 * @param targetFile
	 *            {@link Path}
	 * @throws IOException
	 */
	static void createSymlinkInCurrentWorkDirToPath(Path targetFile) throws IOException
	{
		if (!checkIfSymlinkNameAlreadyCreated(targetFile))
		{
			File symlinkFile = new File("./" + targetFile.getName());
			Files.createSymbolicLink(symlinkFile.toPath(), Paths.get(targetFile.toString()));
			symlinksCreated.add(symlinkFile);
		}
		else
		{
			throw new SymlinkAlreadyExistsException("A symlink already exists for " + targetFile.getName());
		}
	}

	/**
	 * Looks whether a symlink with that name has already been created. Returns {@code true} if a symlink with that name
	 * has already been created, otherwise returns {@code false}.
	 * 
	 * @param targetFile
	 *            {@link Path}
	 * @return {@code boolean}
	 */
	private static boolean checkIfSymlinkNameAlreadyCreated(Path targetFile)
	{
		// Returns true if a symlink with that name has already been created, othw
		for (File symlink : symlinksCreated)
		{
			if (symlink.getName().equals(targetFile.getName()))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Removes all created symlinks.
	 * 
	 * @throws IOException
	 */
	static void removeSymlinksInWorkDir() throws IOException
	{
		for (File symlink : symlinksCreated)
		{
			symlink.delete();
		}
		symlinksCreated.clear();
	}
}

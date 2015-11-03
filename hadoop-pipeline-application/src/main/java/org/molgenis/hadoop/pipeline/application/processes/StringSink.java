package org.molgenis.hadoop.pipeline.application.processes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.molgenis.hadoop.pipeline.application.exceptions.ProcessPipeException;

/**
 * Sink for digesting input streams line-by-line.
 */
public abstract class StringSink extends Sink<String>
{
	/**
	 * Digests the {@code inputStream}. For each line present in the {@code inputStream}, {@code digestStreamItem(item)}
	 * is called. Each {@code item} represents a line as a {@link String}.
	 */
	@Override
	public void handleInputStream(InputStream inputStream)
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while ((line = br.readLine()) != null)
			{
				digestStreamItem(line);
			}
		}
		catch (IOException e)
		{
			throw new ProcessPipeException(e);
		}
		finally
		{
			IOUtils.closeQuietly(br);
		}
	}
}

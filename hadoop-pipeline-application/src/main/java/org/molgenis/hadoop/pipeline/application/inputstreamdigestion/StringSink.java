package org.molgenis.hadoop.pipeline.application.inputstreamdigestion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;

/**
 * Sink for digesting input streams line-by-line.
 */
public abstract class StringSink extends Sink<String>
{
	/**
	 * Digests the {@code inputStream}. For each line present in the {@code inputStream},
	 * {@link #digestStreamItem(String)} is called.
	 */
	@Override
	public void handleInputStream(InputStream inputStream) throws IOException
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
		finally
		{
			IOUtils.closeQuietly(br);
		}
	}

	/**
	 * Digests a single line from the {@link InputStream}. Be sure to create a custom {@code @Override} implementation!
	 * 
	 * @param item
	 *            {@link String}
	 */
	@Override
	protected abstract void digestStreamItem(String item) throws IOException;
}

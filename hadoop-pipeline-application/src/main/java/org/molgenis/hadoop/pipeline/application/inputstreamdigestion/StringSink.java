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
	 * {@link #digestStreamItem(String)} is called. If a custom {@link #digestHeader(String)} is implemented, the first
	 * line will be treated differently. Otherwise, the first line will be treated like the other lines.
	 */
	@Override
	public void handleInputStream(InputStream inputStream) throws IOException
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new InputStreamReader(inputStream));
			String line = br.readLine();
			digestHeader(line);
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
	 * Digests a single line from the {@link InputStream}. Be sure to create a custom {@code @Override} implementation
	 * that defines what should be done for each line (excluding the first line if a custom
	 * {@link #digestHeader(String)} is implemented)!
	 * 
	 * @param item
	 *            {@link String} a line from the {@link InputStream} digested by {@link #handleInputStream(InputStream)}
	 *            .
	 */
	@Override
	protected abstract void digestStreamItem(String item) throws IOException;

	/**
	 * Digests the first line within {@link #handleInputStream(InputStream)} from the {@link InputStream}. By default,
	 * the first line will be treated the same as {@link #digestStreamItem(String)}. If a custom implementation is
	 * created, uses this implementation for the first line instead.
	 * 
	 * @param item
	 *            {@link String} the first line from the {@link InputStream} digested by
	 *            {@link #handleInputStream(InputStream)}.
	 * @throws IOException
	 */
	protected void digestHeader(String item) throws IOException
	{
		digestStreamItem(item);
	};
}

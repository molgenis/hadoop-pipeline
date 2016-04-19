package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.io.InputStream;

import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

/**
 * Reads an alignment reference dictionary file that was added to the distributed cache of a
 * {@link org.apache.hadoop.mapreduce.Job}.
 */
public class HadoopRefSeqDictReader extends HadoopFileReader<SAMSequenceDictionary>
{
	/**
	 * Digests an alignment reference dictionary {@link InputStream} and stores the individual lines with an {@code @SQ}
	 * tag as {@link SAMSequenceRecord}{@code s} that are added to a {@link SAMSequenceDictionary}. Each
	 * {@link SAMSequenceRecord} stores the name and length fields from a specific {@code @SQ} line. Other fields are
	 * omitted. If a single {@code @SQ} line has no name or length field (or the given length is lower than 1), an
	 * {@link IOException} is thrown.
	 * 
	 * @return {@link SAMSequenceDictionary} containing {@link SAMSequenceRecord}{@code s}, each having a value for
	 *         {@link SAMSequenceRecord#getSequenceName()} and {@link SAMSequenceRecord#getSequenceLength()}.
	 */
	@Override
	public SAMSequenceDictionary read(InputStream inputStream) throws IOException
	{
		final SAMSequenceDictionary samSeqDict = new SAMSequenceDictionary();

		StringSink sink = new StringSink()
		{
			@Override
			public void digestStreamItem(String item) throws IOException
			{
				String sqName = null;
				int sqLen = -1;

				if (item.startsWith("@SQ"))
				{
					// Excludes the @SQ tab including the first tab, Splits the rest of the line on tabs and goes
					// through each field.
					String[] splits = item.substring(4).split("\t");
					for (String split : splits)
					{
						// Looks for sequence name.
						if (split.startsWith("SN:"))
						{
							// Throws exception if multiple SN fields present in @SQ tag.
							if (sqName != null)
							{
								throw new IOException("Multiple \"SN\" fields in: " + item.toString());
							}
							sqName = split.substring(3);
						}
						// Looks for sequence length.
						else if (split.startsWith("LN:"))
						{
							// Throws exception if multiple PG fields present in @SQ tag.
							if (sqLen != -1)
							{
								throw new IOException("Multiple \"LN\" fields in: " + item.toString());
							}
							sqLen = Integer.parseInt(split.substring(3));
						}
					}

					// Throws exception if no sequence name or length was found (or if length < 1).
					if (sqName == null || sqLen < 1)
					{
						throw new IOException("Required fields (SN and/or LN) missing or invalid: " + item.toString());
					}

					// Adds the sequence to the sequence record dictionary.
					SAMSequenceRecord seqRecord = new SAMSequenceRecord(sqName, sqLen);
					samSeqDict.addSequence(seqRecord);
				}
			}
		};

		sink.handleInputStream(inputStream);

		return samSeqDict;
	}
}

package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;

import htsjdk.tribble.bed.BEDCodec;
import htsjdk.tribble.bed.BEDCodec.StartOffset;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Read a bed-formatted file that was added to the distributed cache of a {@link org.apache.hadoop.mapreduce.Job}.
 */
public class HadoopBedFormatFileReader extends HadoopFileReader<ArrayList<BEDFeature>>
{
	/**
	 * Turns a BED-formatted {@link InputStream} into an {@link ArrayList}{@code <}{@link BEDFeature}{@code >}. This
	 * BED-format that is 0-based with an exclusive end-position is converted to {@link BEDFeature}{@code s} that are
	 * 1-based and have an inclusive end-position. Uses {@link BEDCodec} for decoding a bed-formatted file. This codec
	 * allows a bed file to only have a contig name and start position without an end position, in which case the start
	 * position will be used as end position as well.
	 * 
	 * @return {@link ArrayList}{@code <}{@link BEDFeature}{@code >}
	 * @see {@link BEDCodec#decode(String[])} -> line 102 & 106
	 * @see <a href=
	 *      'https://genome.ucsc.edu/FAQ/FAQformat.html#format1'>https://genome.ucsc.edu/FAQ/FAQformat.html#format1</a>
	 */
	@Override
	public ArrayList<BEDFeature> read(InputStream inputStream) throws IOException
	{
		// IMPORTANT:
		// BED-format is 0-based, start is inclusive, end is exclusive!
		// BEDFeature is 1-based, start is inclusive, end is inclusive!
		final BEDCodec codec = new BEDCodec(StartOffset.ONE);
		final ArrayList<BEDFeature> bedFeatures = new ArrayList<BEDFeature>();

		StringSink sink = new StringSink()
		{
			@Override
			public void digestStreamItem(String item) throws IOException
			{
				BEDFeature bedFeature = codec.decode(item.trim());

				// If bedFeature is null (for example due to a line from the bed file containing only a contig field),
				// an exception is thrown. Do note that when there is a start field but no end field, the BEDCodec will
				// set the start value as end value and NO exception will be thrown.
				if (bedFeature == null)
				{
					throw new IOException("No bed file instance could be created from: " + item);
				}
				bedFeatures.add(bedFeature);
			}
		};

		sink.handleInputStream(inputStream);

		// Sorts the contigs (file should be sorted anyway but as added safety layer).
		sortBedFeatureArray(bedFeatures);

		return bedFeatures;
	}

	/**
	 * Sorts a given {@link ArrayList} with {@link BEDFeature}{@code s} on the {@link BEDFeature#getStart()}, and if the
	 * start value is the same, uses {@link BEDFeature#getEnd()} for sorting as well.
	 * 
	 * @param bedFeatures
	 *            {@link ArrayList}{@code <}{@link BEDFeature}{@code >}
	 */
	private void sortBedFeatureArray(ArrayList<BEDFeature> bedFeatures)
	{
		Collections.sort(bedFeatures, new Comparator<BEDFeature>()
		{
			public int compare(BEDFeature one, BEDFeature other)
			{
				// Sorts on start value of a contig.
				int c = one.getStart() - other.getStart();
				// If there is no difference in start value, sorts on end value.
				if (c == 0)
				{
					c = one.getEnd() - other.getEnd();
				}
				return c;
			}
		});
	}
}

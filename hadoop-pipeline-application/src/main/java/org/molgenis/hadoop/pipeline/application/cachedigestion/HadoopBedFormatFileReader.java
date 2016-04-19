package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;

import htsjdk.tribble.bed.BEDCodec;
import htsjdk.tribble.bed.BEDCodec.StartOffset;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Read a bed-formatted file that was added to the distributed cache of a {@link org.apache.hadoop.mapreduce.Job}.
 */
public class HadoopBedFormatFileReader extends HadoopFileReader<ContigRegionsMap>
{
	/**
	 * Turns a BED-formatted {@link InputStream} into an {@link List}{@code <}{@link Region}{@code >}. This BED-format
	 * that is 0-based with an exclusive end-position is converted to {@link Region}{@code s} that are 1-based and have
	 * an inclusive end-position. Uses {@link BEDCodec} for decoding a bed-formatted file. This codec allows a bed file
	 * to only have a contig name and start position without an end position, in which case the start position will be
	 * used as end position as well.
	 * 
	 * @return {@link List}{@code <}{@link Region}{@code >}
	 * @see {@link BEDCodec#decode(String[])} -> line 102 & 106
	 * @see <a href= 'https://genome.ucsc.edu/FAQ/FAQformat.html#format1'>https://genome.ucsc.edu/FAQ/FAQformat.html#
	 *      format1</a>
	 */
	@Override
	public ContigRegionsMap read(InputStream inputStream) throws IOException
	{
		// IMPORTANT:
		// BED-format is 0-based, start is inclusive, end is exclusive!
		// BEDFeature is 1-based, start is inclusive, end is inclusive!
		final BEDCodec codec = new BEDCodec(StartOffset.ONE);
		final ContigRegionsMapBuilder map = new ContigRegionsMapBuilder();

		StringSink sink = new StringSink()
		{
			@Override
			protected void digestStreamItem(String item) throws IOException
			{
				BEDFeature bedFeature = codec.decode(item.trim());

				// If bedFeature is null (for example due to a line from the bed file containing only a contig field),
				// an exception is thrown. Do note that when there is a start field but no end field, the BEDCodec will
				// set the start value as end value and NO exception will be thrown.
				if (bedFeature == null)
				{
					throw new IOException("No bed file instance could be created from: " + item);
				}
				map.add(new Region(bedFeature.getContig(), bedFeature.getStart(), bedFeature.getEnd()));
			}
		};

		sink.handleInputStream(inputStream);

		return map.build();
	}
}

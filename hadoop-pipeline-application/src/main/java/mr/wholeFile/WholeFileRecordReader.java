/**
 * Copyright 2014, dimamayteacher
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file has been edited compared to the original. This change is the
 * inclusion of the licensing notice (and this comment). This was done
 * because no NOTICE file was present nor such a license was mentioned
 * in the individual files.
 * 
 * The used license and other relevant information was extracted from
 * https://code.google.com/p/hadoop-course/ (on 6 October 2015).
 * The used year was when the last commit was added (found on
 * https://code.google.com/p/hadoop-course/source/list). As all commits
 * were made with the author name "dimamayteacher", this name was filled in
 * as copyright owner.
 * 
 * Furthermore, comments (line 110-113) have been added to the method
 * getProgress().
 * 
 * Regarding code adjustments, the getCurrentKey() returned a NullWritable
 * originally, but was adjusted to return Text containing the path to
 * the file instead. This also means the extends was changed to
 * <Text, BytesWritable> instead of <NullWritable, BytesWritable>.
 */

package mr.wholeFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable>
{
	private FileSplit split;
	private Configuration conf;

	private final BytesWritable currValue = new BytesWritable();
	private boolean fileProcessed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		this.split = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (fileProcessed)
		{
			return false;
		}

		int fileLength = (int) split.getLength();
		byte[] result = new byte[fileLength];

		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = null;
		try
		{
			in = fs.open(split.getPath());
			IOUtils.readFully(in, result, 0, fileLength);
			currValue.set(result, 0, fileLength);
		}
		finally
		{
			IOUtils.closeStream(in);
		}
		this.fileProcessed = true;
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return new Text(split.getPath().toString());
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException
	{
		return currValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		// Returns 0 as the input split (file) is processed by the mapper as a whole (instead of chunks from the split
		// on which can be reported how many chunks of the total split has been processed). The split is either not
		// processed or completely processed. If input split is completely processed, {@code nextKeyValue()} will return
		// false.

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException
	{
		// nothing to close
	}

}

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
 * Adjusted the code to use <Text, BytesWritable> instead of
 * <NullWritable, BytesWritable>
 */

package mr.wholeFile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable>
{

	@Override
	protected boolean isSplitable(JobContext context, Path filename)
	{
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException
	{
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(inputSplit, context);
		return reader;
	}

}

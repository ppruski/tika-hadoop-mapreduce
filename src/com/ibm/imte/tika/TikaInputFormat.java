package com.ibm.imte.tika;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * The InputFormat class is extending CombineFileInputFormat to efficiently read
 * small files from HDFS. It then sets our TikaRecordReader that will extract
 * the text from the documents.
 */
public class TikaInputFormat extends CombineFileInputFormat<Text, Text>
{
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException
	{

		return new TikaRecordReader((CombineFileSplit) split, context);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file)
	{
		return false;
	}
}

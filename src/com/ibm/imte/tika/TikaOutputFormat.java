package com.ibm.imte.tika;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Our TikaOutputFormat class creates an output stream and sets the
 * TikaRecordWriter. We could have also used the standard TextOutputFormat class
 * and created the strings in the Mapper or Reducer of our job.
 */
public class TikaOutputFormat extends FileOutputFormat<Text, Text>
{

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException
	{
		Path path = getDefaultWorkFile(context, "");
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream output = fs.create(path, context);
		return new TikaRecordWriter(output, context);
	}

}

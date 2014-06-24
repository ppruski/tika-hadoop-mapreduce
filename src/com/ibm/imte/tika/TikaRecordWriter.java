package com.ibm.imte.tika;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * This class takes the key which is the filename and the value which is the
 * text file content and merges them into a delimited text line
 * filename|filecontent. Delimiter is changeable.
 */
public class TikaRecordWriter extends RecordWriter<Text, Text>
{

	private DataOutputStream out;
	private static Logger logger = Logger.getLogger(TikaRecordWriter.class);
	private String delimiter = "|";

	public TikaRecordWriter(DataOutputStream output, TaskAttemptContext context)
	{
		this.out = output;
		String confDelimiter = context.getConfiguration().get(
				"com.ibm.imte.tika.delimiter");
		if (confDelimiter != null)
			delimiter = confDelimiter;
		logger.info("Delimiter character: " + delimiter);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException
	{
		this.out.close();
	}

	public void write(Text key, Text value) throws IOException,
			InterruptedException
	{
		out.writeBytes(key.toString());
		out.writeBytes(delimiter);
		out.writeBytes(value.toString());
		out.writeBytes("\n");
	}

}

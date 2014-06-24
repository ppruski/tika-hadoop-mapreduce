package com.ibm.imte.tika;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * This is the main conversion class. It gets a list of documents in a split and
 * then reads them one by one returning a record with the filename as the key
 * and the file content as the value.
 */
public class TikaRecordReader extends RecordReader<Text, Text>
{

	private CombineFileSplit split;
	private FileSystem fs;
	private Text key, value;
	private Path[] paths;
	private FSDataInputStream currentStream;
	private TikaHelper tikaHelper;

	// count and done are used for progress
	private int count = 0;
	private boolean done = false;

	public TikaRecordReader(CombineFileSplit split, TaskAttemptContext context)
			throws IOException
	{
		this.paths = split.getPaths();
		this.fs = FileSystem.get(context.getConfiguration());
		this.split = split;
		this.tikaHelper = new TikaHelper(context.getConfiguration());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (count >= split.getNumPaths())
		{
			done = true;
			return false; // we have no more data to parse
		}

		Path path = null;
		key = new Text();
		value = new Text();

		try
		{
			path = this.paths[count];
		} catch (Exception e)
		{
			return false;
		}

		currentStream = null;
		currentStream = fs.open(path);

		key.set(path.getName());
		value.set(tikaHelper.readPath(currentStream));

		currentStream.close();
		count++;

		return true; // we have more data to parse
	}

	@Override
	public void close() throws IOException
	{
		done = true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException
	{
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return done ? 1.0f : (float) (count / paths.length);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
	{
	}
}

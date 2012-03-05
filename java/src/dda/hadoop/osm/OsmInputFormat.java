package dda.hadoop.osm;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OsmInputFormat extends FileInputFormat<NullWritable, Text> {
	public RecordReader<NullWritable,Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
		return new OsmRecordReader();
	}
}

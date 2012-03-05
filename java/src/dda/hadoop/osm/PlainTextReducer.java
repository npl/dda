package dda.hadoop.osm;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class PlainTextReducer extends Reducer<Text,Text,NullWritable,Text> {
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<NullWritable,Text>(context);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    for(Text value: values) {
	    	if (key.toString().startsWith("node"))
	    		multipleOutputs.write("nodes", NullWritable.get(), value);
	    	else
	    		multipleOutputs.write("wayparts", NullWritable.get(), value);
	    }
	}
}
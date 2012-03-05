package dda.hadoop.job;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dda.hadoop.osm.OsmInputFormat;
import dda.hadoop.osm.PlainTextMapper;
import dda.hadoop.osm.PlainTextReducer;


public class Xml2PlainTextJob extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length == 2) {
			Job job = new Job(getConf());

			job.setJarByClass(Xml2PlainTextJob.class);
			job.setJobName("Xml2PlainTextJob");

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setInputFormatClass(OsmInputFormat.class);

			job.setMapperClass(PlainTextMapper.class);
			job.setReducerClass(PlainTextReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(120);
			
			MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "wayparts", TextOutputFormat.class, Text.class, Text.class);

			try {
				job.waitForCompletion(true);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			return 0;
		} else {
			System.out.println("usage: Xml2PlainTextJob <input-file> <output-dir>");
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), new Xml2PlainTextJob(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

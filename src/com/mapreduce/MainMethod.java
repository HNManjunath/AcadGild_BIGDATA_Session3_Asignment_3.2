package com.mapreduce;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class MainMethod {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();	//creating configuration object
		Job job = new Job(conf, "MaponlyTask");
		job.setJarByClass(MainMethod.class);		//Setting Mapper class object
		
		job.setMapOutputKeyClass(LongWritable.class);		//setting map output key
		job.setMapOutputValueClass(LongWritable.class);		//setting map output value
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(MapperClass.class);
		//job.setReducerClass(Task1Reducer.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); //setting input path .. data file path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));	// setting output path
		
		job.setNumReduceTasks(0);							//setting Reducer task to zero, means it won't take reducer class.
		
		
		job.waitForCompletion(true);
	}
}

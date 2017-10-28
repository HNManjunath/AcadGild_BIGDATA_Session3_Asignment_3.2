package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class MapperClass extends Mapper<LongWritable, Text, NullWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|"); // getting the complete line data separated by |
		Text companyName = new Text((lineArray[0])); // getting company name
		Text productName = new Text((lineArray[1])); // getting product name

		if (!(companyName.toString().equalsIgnoreCase("NA")) && !(productName.toString().equalsIgnoreCase("NA"))) { // checking
																													// for
																													// invalid
																													// record
			context.write(NullWritable.get(), value);
		}
	}
}

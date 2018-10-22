package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.opencsv.CSVParser;

public class WEducationReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		String[] parts = new CSVParser().parseLine(key.toString());
		double percent = 0;
		if(!parts[60].equals("")){
			percent = Double.parseDouble(parts[60]);
		}
		if(percent < 30.0 && percent != 0){
			context.write(new Text(parts[0]),new DoubleWritable(percent));
		}
	}
}


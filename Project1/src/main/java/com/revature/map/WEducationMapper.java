package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WEducationMapper extends Mapper<Object,Text, Text,IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.contains("SE.")){
			context.write(value, new IntWritable(1));
		}
	}

}

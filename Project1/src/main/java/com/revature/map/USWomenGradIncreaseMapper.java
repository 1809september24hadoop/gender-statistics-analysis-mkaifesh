package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USWomenGradIncreaseMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

	}

}
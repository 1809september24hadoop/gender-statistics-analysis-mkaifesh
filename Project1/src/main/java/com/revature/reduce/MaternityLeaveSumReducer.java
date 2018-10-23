package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaternityLeaveSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int max = Integer.MIN_VALUE;
		for(IntWritable value: values){
			if(value.get() > max){
				max = value.get();
			}
		}
		context.write(key, new IntWritable(max));
	}
}

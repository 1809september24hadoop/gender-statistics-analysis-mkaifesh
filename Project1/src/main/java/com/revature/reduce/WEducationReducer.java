package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WEducationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double graduateRate = 0D;
		for(DoubleWritable value: values){
			graduateRate = value.get();
			if(graduateRate < 30){
				context.write(key,new DoubleWritable(graduateRate));
			}
		}
	}
	
}


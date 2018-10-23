package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaternityLeaveMaxReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
	public static volatile String CURRENT_MAX_COUNTRY = null;
	public static volatile int CURRENT_MAX_LEAVE = Integer.MIN_VALUE;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		for(IntWritable value: values){
			if(value.get() > CURRENT_MAX_LEAVE){
				CURRENT_MAX_LEAVE = value.get();
				CURRENT_MAX_COUNTRY = key.toString();
			}
		}
		//context.write(new Text(CURRENT_MAX_COUNTRY), new IntWritable(CURRENT_MAX_LEAVE));
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		context.write(new Text(CURRENT_MAX_COUNTRY), new IntWritable(CURRENT_MAX_LEAVE));
	}
}

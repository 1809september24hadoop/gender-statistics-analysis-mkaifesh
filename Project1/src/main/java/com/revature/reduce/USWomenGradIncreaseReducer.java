package com.revature.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USWomenGradIncreaseReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context){
		
	}
}

package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WEducationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double sumGraduateRate = 0D;
		int totalValues = 0;
		for(DoubleWritable value: values){
			sumGraduateRate += value.get();
			++totalValues;
		}
		double averageGraduateRate = sumGraduateRate / totalValues;
		if((averageGraduateRate) < 30){
			context.write(key, new DoubleWritable(averageGraduateRate));
		}
	}
	
}


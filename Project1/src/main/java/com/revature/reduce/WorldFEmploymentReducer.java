package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.util.concurrent.AtomicDouble;

public class WorldFEmploymentReducer extends Reducer<Text,DoubleWritable,Text,Text>{
	static AtomicDouble currentYear = new AtomicDouble(0.0);
	static AtomicDouble previousYear = new AtomicDouble(0.0);
	static AtomicDouble percentChange = new AtomicDouble(0.0);
	static AtomicDouble runningPercentChange = new AtomicDouble(0.0);
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
		StringBuilder sb = new StringBuilder();
		boolean firstValue = true;
		for(DoubleWritable value: values){
			if(firstValue){
				firstValue = false;
				currentYear = new AtomicDouble(value.get());
				sb.append(Double.toString(value.get())+"%, ");
			}
			else{
				previousYear = new AtomicDouble(currentYear.get());
				currentYear = new AtomicDouble(value.get());
				percentChange = new AtomicDouble( ((currentYear.get() - previousYear.get())/previousYear.get())*100);
				sb.append(Double.toString(percentChange.get())+", ");
			}
		}
		context.write(new Text("Percent Change in Female Employment since 2000: "+key),new Text(sb.toString()));
	}

}

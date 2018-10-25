package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;

public class WorldFEmploymentReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	static AtomicDouble firstPercentValue = new AtomicDouble(0.0);
	static AtomicDouble runningIncrease = new AtomicDouble(0.0);
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
		boolean firstValue = true;
		for(DoubleWritable value: values){
			if(firstValue){
				firstValue = false;
				firstPercentValue = new AtomicDouble(value.get());
			}
			else{
				runningIncrease = new AtomicDouble(value.get()-runningIncrease.get());
			}
		}
		context.write(new Text("Increase in Female employment percentage since 2000: "+key), new DoubleWritable(-1*runningIncrease.get()));
	}
}

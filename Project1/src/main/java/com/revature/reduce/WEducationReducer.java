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
	
	public static void main(String[] args) {
		String test = "\"Congo, Dem. Rep.\",\"COD\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.30557\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.63175\",\"\",\"\",\"\",";
		String trimmed = test.substring(1, test.length() - 2);
		System.out.println(trimmed);
		String[] parts = trimmed.split("\",\"");
		int index = 0;
		for(String part: parts){
			System.out.println("part["+index+"]: "+part);
			++index;
		}
		
	}
}


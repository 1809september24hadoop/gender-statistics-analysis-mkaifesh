package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaternityLeaveMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String trimedLine = line.substring(1, line.length() - 2);
		String[] parts = trimedLine.split("\",\"");
		String country = parts[0];
		if(parts[3].equals("SH.MMR.LEVE")){
			for(int i = 4; i < parts.length;i++){
				String yearData = parts[i];
				if(!yearData.equals("")){
					context.write(new Text(country), new IntWritable(Integer.parseInt(yearData)));
				}
			}
		}
	}
}

package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorldFEmploymentMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		String line = value.toString();
		String trimedLine = line.substring(1, line.length() - 1);
		String[] parts = trimedLine.split("\",\"");
		parts[60] = parts[60].replaceAll("\"", "");
		String country = parts[0];
		if(parts[3].equals("SL.EMP.TOTL.SP.FE.ZS")){
			for(int i = 44; i < parts.length; i++){
				String yearData = parts[i];
				if(!yearData.equals("")){
					context.write(new Text(country), new DoubleWritable(Double.parseDouble(yearData)));
				}
			}
		}
	}
}


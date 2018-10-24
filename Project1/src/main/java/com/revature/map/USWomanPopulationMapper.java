package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USWomanPopulationMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException{
		String line = value.toString();
		String trimedLine = line.substring(1, line.length() - 1);
		String[] parts = trimedLine.split("\",\"");
		parts[60] = parts[60].replaceAll("\"", "");
		
		int currentYear = 2000;
		if(parts[0].equals("United States") && parts[3].equals("SP.POP.TOTL.FE.IN")){
			for(int i = 44; i < parts.length;++i){
				if(parts[i].equals("")){
					++currentYear;
					continue;
				}
				context.write(new Text("United States:"+currentYear),new Text("population\t"+parts[i]));
				++currentYear;
			}
		}
	}
}

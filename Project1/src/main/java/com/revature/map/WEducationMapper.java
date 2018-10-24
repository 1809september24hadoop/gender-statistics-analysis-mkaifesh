package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WEducationMapper extends Mapper<LongWritable, Text, Text,DoubleWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String trimedLine = line.substring(1, line.length() - 1);
			String[] parts = trimedLine.split("\",\"");
			parts[60] = parts[60].replaceAll("\"", "");
			int currentYear = 1960;
			String country = parts[0];
			if(parts[3].equals("SE.TER.HIAT.BA.FE.ZS")){
				for(int i = 4; i < parts.length; i++){
					String yearData = parts[i];
					if(!yearData.equals("")){
						context.write(new Text(country+":"+currentYear), new DoubleWritable(Double.valueOf(yearData)));
					}
					++currentYear;
				}
			}
	}
}

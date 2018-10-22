package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;


public class WEducationMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] parts = new CSVParser().parseLine(value.toString());
			if(parts[3].equals("SE.TER.CUAT.BA.FE.ZS")){
				context.write(value, new IntWritable(1));
			}
	}
}

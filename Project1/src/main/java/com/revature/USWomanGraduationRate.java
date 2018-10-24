package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.USGraduationRateMapper;
import com.revature.map.USWomanPopulationMapper;
import com.revature.reduce.USWomanGraduationPopulationReducer;




public class USWomanGraduationRate {
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println("Usage: <InputFile> <outputDirectory>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(USWomanGraduationRate.class);
		job.setJobName("US Woman Graduation Increase");
		job.setReducerClass(USWomanGraduationPopulationReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,USGraduationRateMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,USWomanPopulationMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		
	}
}

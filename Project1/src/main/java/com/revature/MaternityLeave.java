package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.revature.map.MaternityLeaveMapper;
import com.revature.reduce.MaternityLeaveMaxReducer;
import com.revature.reduce.MaternityLeaveSumReducer;


public class MaternityLeave {
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println("Usage: <Input file>  <Output Directory>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(MaternityLeave.class);
		job.setJobName("Maternity Leave");
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MaternityLeaveMapper.class);
		job.setCombinerClass(MaternityLeaveSumReducer.class);
		job.setReducerClass(MaternityLeaveMaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
	}
}

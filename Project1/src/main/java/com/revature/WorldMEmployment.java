package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WorldMEmploymentMapper;
import com.revature.reduce.WorldMEmploymentReducer;



public class WorldMEmployment {
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(WorldMEmployment.class);
		job.setMapperClass(WorldMEmploymentMapper.class);
		job.setReducerClass(WorldMEmploymentReducer.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		
	}
}

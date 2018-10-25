package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WorldFEmploymentMapper;
import com.revature.reduce.WorldFEmploymentReducer;

public class WorldFEmployment {
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(WorldFEmployment.class);
		job.setMapperClass(WorldFEmploymentMapper.class);
		job.setReducerClass(WorldFEmploymentReducer.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		
	}
}

package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WorldFEmploymentMapper;
import com.revature.reduce.WorldFEmploymentReducer;

/**
 * This Job maps/reduces employment changes between Women on a year to year 
 * basis between countries, starting from 2000
 * 
 * Thought Process:
 *   -Map all countries by "SL.EMP.TOTL.SP.FE.ZS"
 *   (Employment to population ratio, 15+, female (%) (modeled ILO estimate))
 *   -In the reducer, determine the percent change between consecutive years and
 *   add each to the return value
 *   
 * Assumptions:
 *   -For the most accurate and complete data, all years from 2000 to 2016 have a percent
 *   value in this field
 * @author cloudera
 *
 */

public class WorldFEmployment {
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(WorldFEmployment.class);
		job.setMapperClass(WorldFEmploymentMapper.class);
		job.setReducerClass(WorldFEmploymentReducer.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		
	}
}

package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WorldMEmploymentMapper;
import com.revature.reduce.WorldMEmploymentReducer;

/**
 * This Job maps/reduces employment changes between Men on a year to year 
 * basis between countries, starting from 2000
 * 
 * Thought Process:
 *   -Map all countries by "SL.EMP.TOTL.SP.MA.ZS"
 *   (Employment to population ratio, 15+, male (%) (modeled ILO estimate))
 *   -In the reducer, determine the percent change between consecutive years and
 *   add each to the return value
 *   
 * Assumptions:
 *   -For the most accurate and complete data, all years from 2000 to 2016 have a percent
 *   value in this field
 * @author cloudera
 *
 */

public class WorldMEmployment {
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(WorldMEmployment.class);
		job.setMapperClass(WorldMEmploymentMapper.class);
		job.setReducerClass(WorldMEmploymentReducer.class);
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

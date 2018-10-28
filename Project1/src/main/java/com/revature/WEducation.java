package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.revature.map.WEducationMapper;
import com.revature.reduce.WEducationReducer;

/**
 * This Job maps/reduces all countries by "SE.TER.HIAT.BA.FE.ZS"
 * (Educational attainment, 
 * completed Bachelor's or equivalent, population 25+ years, female (%)
 * 
 * Thought Process:
 *   -Map all countries by "SE.TER.HIAT.BA.FE.ZS"
 *   -Reduce these to only the countries that have an average value of < 30% (based on given data)
 * 
 * Assumptions:
 *   -A "graduate" is considered someone who has obtained a Bachelor's degree or equivalent
 *   -For the most accurate data, all years have a percentage value(although this is handled by map/reduce logic)
 *   -If there no data is given, then the year is not included in the average calculation
 * @author cloudera
 *
 */

public class WEducation {
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.printf("Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(WEducation.class);
		job.setJobName("Women Education");
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WEducationMapper.class);
		job.setReducerClass(WEducationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		
	}
}

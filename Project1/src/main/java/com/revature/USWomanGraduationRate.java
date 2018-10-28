package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * This Job maps/reduces the United States Female population and graduation percentage to
 * show the number of females with a Bachelor's degree or equivalent
 * 
 * Thought Process:
 *   -Map the United States female population
 *   (Country == "United States" && Indicator Code =="SP.POP.TOTL.FE.IN")
 *   alongside the percentage of females that are "graduates" (Country == "United States" && Indicator Code =="SE.TER.HIAT.BA.FE.ZS"
 *   by year
 *   
 *   -Reduce this set by calculating the amount of women with a bachelor's degree, compared to the population for that year
 *   
 *   Assumptions:
 *     -If either the population, or the percentage of women with bachelor's is missing, we can not determine accurate data
 *     -A "graduate" is a woman with only a bachelor's degree or equivalent
 *     -The ratio considers women that earned a degree from the previous year
 * @author cloudera
 *
 */
public class USWomanGraduationRate {


	public static class USGraduationRateMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String trimedLine = line.substring(1, line.length() - 1);
			String[] parts = trimedLine.split("\",\"");
			parts[60] = parts[60].replaceAll("\"", "");
			int currentYear = 2000;
			if(parts[3].equals("SE.TER.HIAT.BA.FE.ZS") && parts[0].equals("United States")){
				for(int i = 44; i < parts.length; i++){
					if(parts[i].equals("")){
						parts[i] = Integer.toString(Integer.MIN_VALUE);
					}
					context.write(new Text("United States:"+currentYear), new Text("percent\t"+parts[i]));
					++currentYear;
				}
			}
			else if(parts[3].equals("SP.POP.TOTL.FE.IN") && parts[0].equals("United States")){
				for(int i = 44; i < parts.length;++i){
					if(parts[i].equals("")){
						parts[i] = Integer.toString(Integer.MIN_VALUE);
					}
					context.write(new Text("United States:"+currentYear),new Text("population\t"+parts[i]));
					++currentYear;
				}
			}
		}

	}

	public static class USWomanGraduationPopulationReducer extends Reducer<Text, Text, Text, Text>{
		public static final String NO_DATA = Integer.toString(Integer.MIN_VALUE);
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
			double graduationRate = 0;
			int yearPopulation = 0; 
			int numberOfGraduates = 0;
			for(Text t: values){
				String[] parts = t.toString().split("\t");
				if(parts[0].equals("percent")){
					if(parts[1].equals(NO_DATA)){
						context.write(key, new Text("N/A"));
					}
					graduationRate = Double.parseDouble(parts[1]);
				}
				else if(parts[0].equals("population")){
					if(parts[1].equals(NO_DATA)){
						context.write(key, new Text("N/A"));
						return;
					}
					yearPopulation = Integer.parseInt(parts[1]);
				}
			}
			numberOfGraduates = (int) ((graduationRate/100) * yearPopulation);
			if(numberOfGraduates > 0){
				context.write(key, new Text(Integer.toString(numberOfGraduates)));
			}
		}
	}


	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println("Usage: <InputFile> <outputDirectory>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(USWomanGraduationRate.class);
		job.setJobName("US Woman Graduation Increase");
		job.setMapperClass(USGraduationRateMapper.class);
		job.setReducerClass(USWomanGraduationPopulationReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);

	}
}

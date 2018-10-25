package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




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
						continue;
					}
					graduationRate = Double.parseDouble(parts[1]);
				}
				else if(parts[0].equals("population")){
					if(parts[1].equals(NO_DATA)){
						continue;
					}
					yearPopulation = Integer.parseInt(parts[1]);
				}
			}
			numberOfGraduates = (int) ((graduationRate/100) * yearPopulation);
			if(numberOfGraduates > 0){
				context.write(new Text("United States"), new Text(Integer.toString(numberOfGraduates)));
			}
		}
	}

	public static class USTotalFemaleGraduatesReducer extends Reducer<Text,Text,Text,Text>{
		public static final String NO_DATA = Integer.toString(Integer.MIN_VALUE);
		public static final long POPULATION2000 = 142965028; 
		public void reduce(Text key, Iterable<Text>values, Context context)throws IOException,InterruptedException{
			long total = 0;
			for(Text t: values){
				if(t.toString().equals("0.0")){
					continue;
				}
				total += Long.parseLong(t.toString());
			}
			context.write(new Text("The total number of women who recieved a Bachelor's Degree since 2000 (Based on Available Data)"), new Text(Long.toString(total)));
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
		job.setCombinerClass(USWomanGraduationPopulationReducer.class);
		job.setReducerClass(USTotalFemaleGraduatesReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);

	}
}

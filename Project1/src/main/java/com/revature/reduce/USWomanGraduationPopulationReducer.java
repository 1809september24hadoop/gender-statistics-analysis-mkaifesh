package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USWomanGraduationPopulationReducer extends Reducer<Text, Text, Text, Text>{
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
				yearPopulation = Integer.parseInt(parts[1]);
			}
		}
		numberOfGraduates = (int) ((graduationRate/100) * yearPopulation);
		context.write(new Text("United States"), new Text(Double.toString(numberOfGraduates)));
	}
}

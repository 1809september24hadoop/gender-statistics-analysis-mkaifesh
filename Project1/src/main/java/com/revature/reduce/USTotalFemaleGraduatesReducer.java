package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USTotalFemaleGraduatesReducer extends Reducer<Text,Text,Text,Text>{
	public static final String NO_DATA = Integer.toString(Integer.MIN_VALUE);
	public void reduce(Text key, Iterable<Text>values, Context context)throws IOException,InterruptedException{
		long total = 0;
		for(Text t: values){
			if(t.toString().equals(new Text(NO_DATA))){
				continue;
			}
		}
	}
}

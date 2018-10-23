package com.revature.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MaternityLeaveMapper;
import com.revature.reduce.MaternityLeaveMaxReducer;
import com.revature.reduce.MaternityLeaveSumReducer;
public class MaxMaternityLeaveTest {
	private MapDriver<LongWritable,Text,Text,IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private ReduceDriver<Text,IntWritable,Text,IntWritable> combiner;
	private Text test;
	
	@Before
	public void setup(){
		MaternityLeaveMapper mapper = new MaternityLeaveMapper();
		MaternityLeaveSumReducer combiner = new MaternityLeaveSumReducer();
		MaternityLeaveMaxReducer reducer = new MaternityLeaveMaxReducer();
		mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable,Text,Text,IntWritable,Text,IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.setReducer(reducer);
		test = new Text("")
	}
	
	@Test
	public void MapperTest(){
		
	}
	
	@Test
	public void ReducerTest(){
		
	}
	
	@Test
	public void CombinerTest(){
		
	}
	@Test
	public void MapReduceCombineTest(){
		
	}
}

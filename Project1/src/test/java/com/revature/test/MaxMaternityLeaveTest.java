package com.revature.test;

import java.util.ArrayList;
import java.util.List;

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
	private ReduceDriver<Text,IntWritable,Text,IntWritable> combineReduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
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
		combineReduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable,Text,Text,IntWritable,Text,IntWritable>();
		combineReduceDriver.setReducer(combiner);
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.setReducer(reducer);
		test = new Text("\"Bolivia\",\"BOL\",\"Maternity leave (days paid)\",\"SH.MMR.LEVE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"90\",\"\",\"90\",\"\",\"90\",\"\",\"90\",\"\",");
	}
	
	@Test
	public void MapperTest(){
		mapDriver.withInput(new LongWritable(1),test);
		mapDriver.addOutput(new Text("Bolivia"), new IntWritable(90));
		mapDriver.addOutput(new Text("Bolivia"), new IntWritable(90));
		mapDriver.addOutput(new Text("Bolivia"), new IntWritable(90));
		mapDriver.addOutput(new Text("Bolivia"), new IntWritable(90));
		mapDriver.runTest();
	}
	
	@Test
	public void ReducerTest(){
		List<IntWritable> bulValues = new ArrayList<>();
		List<IntWritable> iranValues = new ArrayList<>();
		bulValues.add(new IntWritable(90));
		iranValues.add(new IntWritable(39));
		reduceDriver.withInput(new Text("Iran"), iranValues);
		reduceDriver.withInput(new Text("Bolivia"), bulValues);
		reduceDriver.withOutput(new Text("Bolivia"), new IntWritable(90));
		reduceDriver.runTest();
	}
	
	@Test
	public void CombinerTest(){
		List<IntWritable>values = new ArrayList<>();
		values.add(new IntWritable(10));
		values.add(new IntWritable(5));
		values.add(new IntWritable(40));
		combineReduceDriver.withInput(new Text("Bulgaria"), values);
		combineReduceDriver.withOutput(new Text("Bulgaria"), new IntWritable(40));
		combineReduceDriver.runTest();
	}
	@Test
	public void MapReduceCombineTest(){
		//mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.addInput(new LongWritable(1),test);
		String test2 = "\"Central African Republic\",\"CAF\",\"Maternity leave (days paid)\",\"SH.MMR.LEVE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"40\",\"\",";
		mapReduceDriver.addInput(new LongWritable(2),new Text(test2));
		mapReduceDriver.withOutput(new Text("Bolivia"), new IntWritable(90));
		mapReduceDriver.withOutput(new Text("Bolivia"), new IntWritable(90));
		mapReduceDriver.runTest();
	}
}

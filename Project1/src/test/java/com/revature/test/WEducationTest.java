package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WEducationMapper;
import com.revature.reduce.WEducationReducer;

public class WEducationTest {
	private MapDriver<LongWritable,Text,Text,DoubleWritable>mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private Text test;
	
	@Before
	public void setUp(){
		WEducationMapper mapper = new WEducationMapper();
		mapDriver = new MapDriver<LongWritable,Text,Text,DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		WEducationReducer reducer = new WEducationReducer();
		reduceDriver = new ReduceDriver<Text,DoubleWritable,Text,DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable,Text,Text,DoubleWritable,Text,DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		test = new Text("\"Zimbabwe\",\"ZWE\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.51097\",\"0.88415\",\"3.37858\",");
	}
	
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), test);
		mapDriver.addOutput(new Text("Zimbabwe:2014"),new DoubleWritable(10.51097D));
		mapDriver.addOutput(new Text("Zimbabwe:2015"),new DoubleWritable(0.88415));
		mapDriver.addOutput(new Text("Zimbabwe:2016"),new DoubleWritable(3.37858D));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(10.51097D));
		reduceDriver.withInput(new Text("Zimbabwe:2014"),values);;
		reduceDriver.withOutput(new Text("Zimbabwe:2014"),values.get(0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable(1),test);
		mapReduceDriver.addOutput(new Text("Zimbabwe:2014"), new DoubleWritable(10.51097D));
		mapReduceDriver.addOutput(new Text("Zimbabwe:2015"), new DoubleWritable(0.88415D));
		mapReduceDriver.addOutput(new Text("Zimbabwe:2016"),new DoubleWritable(3.37858D));
		mapReduceDriver.runTest();
	}
}

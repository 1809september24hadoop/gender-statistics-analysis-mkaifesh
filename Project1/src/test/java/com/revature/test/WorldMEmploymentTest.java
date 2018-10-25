package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WorldMEmploymentMapper;
import com.revature.reduce.WorldMEmploymentReducer;

public class WorldMEmploymentTest {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable,Text,Text,DoubleWritable,Text,Text>mapReduceDriver;
	
	@Before
	public void setup(){
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		reduceDriver = new ReduceDriver<Text,DoubleWritable,Text,Text>();
		mapReduceDriver = new MapReduceDriver<LongWritable,Text,Text,DoubleWritable,Text,Text>();
		WorldMEmploymentMapper mapper = new WorldMEmploymentMapper();
		WorldMEmploymentReducer reducer = new WorldMEmploymentReducer();
		mapDriver.setMapper(mapper);
		reduceDriver.setReducer(reducer);
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
		
	}
	
	@Test
	public void TestMapper(){
		Text test = new Text("\"Colombia\",\"COL\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"71.5189971923828\",\"72.4240036010742\",\"73.7539978027344\",\"74.1080017089844\",\"74.1350021362305\",\"72.4219970703125\",\"72.8519973754883\",\"71.427001953125\",\"68.5699996948242\",\"68.7149963378906\",\"72.2440032958984\",\"72.1070022583008\",\"73.7539978027344\",\"72.6419982910156\",\"73.7610015869141\",\"71.2760009765625\",\"71.2009963989258\",\"71.6890029907227\",\"72.7289962768555\",\"73.0350036621094\",\"73.5510025024414\",\"73.8850021362305\",\"73.9589996337891\",\"74.197998046875\",\"74.4300003051758\",\"73.8980026245117\",");
		mapDriver.setInput(new LongWritable(1),test);
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(68.7149963378906));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(72.2440032958984));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(72.1070022583008));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.7539978027344));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(72.6419982910156));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.7610015869141));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(71.2760009765625));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(71.2009963989258));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(71.6890029907227));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(72.7289962768555));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.0350036621094));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.5510025024414));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.8850021362305));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.9589996337891));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(74.197998046875));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(74.4300003051758));
		mapDriver.withOutput(new Text("Colombia"), new DoubleWritable(73.8980026245117));
		mapDriver.runTest();
		
	}
	
	@Test
	public void TestReducer(){
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(10));
		values.add(new DoubleWritable(30));
		values.add(new DoubleWritable(5));
		String expected = "10.0%, 200.0, -83.33333333333334, ";
		reduceDriver.withInput(new Text("Colombia"), values);
		reduceDriver.addOutput(new Text("Percent Change in Male Employment since 2000: Colombia"),new Text(expected));
		reduceDriver.runTest();
		
	}
	@Test
	public void TestMapReduce(){
		mapReduceDriver.addInput(new LongWritable(1), new Text("\"Colombia\",\"COL\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10\",\"30\",\"5\","));
		String expected = "10.0%, 200.0, -83.33333333333334, ";
		mapReduceDriver.withOutput(new Text("Percent Change in Male Employment since 2000: Colombia"), new Text(expected));
		mapReduceDriver.runTest();
	}
}

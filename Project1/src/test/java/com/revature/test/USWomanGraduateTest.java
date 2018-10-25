package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.USWomanGraduationRate.USGraduationRateMapper;
import com.revature.USWomanGraduationRate.USTotalFemaleGraduatesReducer;
import com.revature.USWomanGraduationRate.USWomanGraduationPopulationReducer;

public class USWomanGraduateTest {
	private MapDriver<LongWritable,Text,Text,Text> populationMapDriver;
	private MapDriver<LongWritable,Text,Text,Text> graduationMapDriver;
	private ReduceDriver<Text,Text,Text,Text> reduceDriver;
	private MapReduceDriver<LongWritable,Text,Text,Text,Text,Text> mapReduceDriver;
	private ReduceDriver<Text,Text,Text,Text> combinerDriver;
	
	@Before
	public void setup(){
		populationMapDriver = new MapDriver<LongWritable,Text,Text,Text>();
		graduationMapDriver = new MapDriver<LongWritable,Text,Text,Text>();
		reduceDriver = new ReduceDriver<Text,Text,Text,Text>();
		mapReduceDriver = new MapReduceDriver<LongWritable,Text,Text,Text,Text,Text>();
		combinerDriver = new ReduceDriver<Text,Text,Text,Text>();
		
		USGraduationRateMapper gradMapper = new USGraduationRateMapper();
		USWomanGraduationPopulationReducer reducer = new USWomanGraduationPopulationReducer();
		USTotalFemaleGraduatesReducer combiner = new USTotalFemaleGraduatesReducer();
		
		
		reduceDriver.setReducer(reducer);
		populationMapDriver.setMapper(gradMapper);
		graduationMapDriver.setMapper(gradMapper);
		combinerDriver.setReducer(combiner);
	}
	
	@Test
	public void TestPopulationMapper(){
		Text min= new Text(Integer.toString(Integer.MIN_VALUE));
		Text test = new Text("\"United States\",\"USA\",\"Population, female\",\"SP.POP.TOTL.FE.IN\",\"91080334\",\"92648728\",\"94131326\",\"95546380\",\"96941287\",\"98229423\",\"99451832\",\"100632710\",\"101736800\",\"102820820\",\"104093569\",\"105464260\",\"106627368\",\"107669811\",\"108684186\",\"109803261\",\"110913877\",\"112112212\",\"113387672\",\"114715789\",\"115869519\",\"117033295\",\"118152917\",\"119222596\",\"120241640\",\"121300453\",\"122424625\",\"123527093\",\"124655090\",\"125826547\",\"127224739\",\"128883111\",\"130611654\",\"132260732\",\"133802158\",\"135314671\",\"136809762\",\"138380165\",\"139919288\",\"141456191\",\"142965028\",\"144318117\",\"145599896\",\"146800080\",\"148116767\",\"149449120\",\"150869053\",\"152292131\",\"153723121\",\"155052916\",\"156317021\",\"157460944\",\"158596747\",\"159697341\",\"160883270\",\"162100917\",\"\",");
		populationMapDriver.withInput(new LongWritable(1), test);
		populationMapDriver.withOutput(new Text("United States:2000"),new Text("population\t"+"142965028"));
		populationMapDriver.withOutput(new Text("United States:2001"),new Text("population\t"+"144318117"));
		populationMapDriver.withOutput(new Text("United States:2002"),new Text("population\t"+"145599896"));
		populationMapDriver.withOutput(new Text("United States:2003"),new Text("population\t"+"146800080"));
		populationMapDriver.withOutput(new Text("United States:2004"),new Text("population\t"+"148116767"));
		populationMapDriver.withOutput(new Text("United States:2005"),new Text("population\t"+"149449120"));
		populationMapDriver.withOutput(new Text("United States:2006"),new Text("population\t"+"150869053"));
		populationMapDriver.withOutput(new Text("United States:2007"),new Text("population\t"+"152292131"));
		populationMapDriver.withOutput(new Text("United States:2008"),new Text("population\t"+"153723121"));
		populationMapDriver.withOutput(new Text("United States:2009"),new Text("population\t"+"155052916"));
		populationMapDriver.withOutput(new Text("United States:2010"),new Text("population\t"+"156317021"));
		populationMapDriver.withOutput(new Text("United States:2011"),new Text("population\t"+"157460944"));
		populationMapDriver.withOutput(new Text("United States:2012"),new Text("population\t"+"158596747"));
		populationMapDriver.withOutput(new Text("United States:2013"),new Text("population\t"+"159697341"));
		populationMapDriver.withOutput(new Text("United States:2014"),new Text("population\t"+"160883270"));
		populationMapDriver.withOutput(new Text("United States:2015"),new Text("population\t"+"162100917"));
		populationMapDriver.withOutput(new Text("United States:2016"),new Text("population\t"+min));
		populationMapDriver.runTest();
		
	}
	@Test
	public void TestGraduationMapper(){
		Text min= new Text(Integer.toString(Integer.MIN_VALUE));
		Text test = new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\",");
		graduationMapDriver.withInput(new LongWritable(1),test);
		graduationMapDriver.withOutput(new Text("United States:2000"), new Text("percent\t"+min));
		graduationMapDriver.withOutput(new Text("United States:2001"), new Text("percent\t"+min));
		graduationMapDriver.withOutput(new Text("United States:2002"), new Text("percent\t"+min));
		graduationMapDriver.withOutput(new Text("United States:2003"), new Text("percent\t"+min));
		graduationMapDriver.withOutput(new Text("United States:2004"), new Text("percent\t"+"35.37453"));
		graduationMapDriver.withOutput(new Text("United States:2005"), new Text("percent\t"+"36.00504"));
		graduationMapDriver.withOutput(new Text("United States:2006"), new Text("percent\t"+"37.52263"));
		graduationMapDriver.withOutput(new Text("United States:2007"), new Text("percent\t"+min));
		graduationMapDriver.withOutput(new Text("United States:2008"), new Text("percent\t"+"38.44067"));
		graduationMapDriver.withOutput(new Text("United States:2009"), new Text("percent\t"+"39.15297"));
		graduationMapDriver.withOutput(new Text("United States:2010"), new Text("percent\t"+"39.89922"));
		graduationMapDriver.withOutput(new Text("United States:2011"), new Text("percent\t"+"40.53132"));
		graduationMapDriver.withOutput(new Text("United States:2012"), new Text("percent\t"+"41.12231"));
		graduationMapDriver.withOutput(new Text("United States:2013"), new Text("percent\t"+"20.18248"));
		graduationMapDriver.withOutput(new Text("United States:2014"), new Text("percent\t"+"20.38445"));
		graduationMapDriver.withOutput(new Text("United States:2015"), new Text("percent\t"+"20.68499"));
		graduationMapDriver.withOutput(new Text("United States:2016"), new Text("percent\t"+min));
		graduationMapDriver.runTest();
	}
	@Test
	public void TestGraduationPopulationReducer(){
		List<Text> values = new ArrayList<>();
		values.add(new Text("population\t20000"));
		values.add(new Text ("percent\t20.5"));
		reduceDriver.withInput(new Text("United States:"),values);
		reduceDriver.withOutput(new Text("United States"), new Text("4100"));
		reduceDriver.runTest();
	}
	
	@Test
	public void TestGraduateNumberCombiner(){
		List<Text> values = new ArrayList<>();
		values.add(new Text("4100"));
		values.add(new Text("0"));
		combinerDriver.withInput(new Text("United States"), values);
		combinerDriver.withOutput(new Text("The total number of women who recieved a Bachelor's Degree since 2000 (Based on Available Data)"), new Text("4100"));
		combinerDriver.runTest();
	}
}

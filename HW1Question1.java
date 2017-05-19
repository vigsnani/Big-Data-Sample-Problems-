package JavaHDFS.JavaHDFS;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class HW1Question1{
	public static class map1 extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] data= new String[3];
				data=value.toString().split("::");
			if(data[1].contains("Palo Alto")){
				String s= data[2].substring(5); 
				char arr[]= s.toCharArray();
				String t="";
				for(int i=0;i<arr.length-1;i++)
				t+=arr[i];
				String [] array= t.trim().split(",");
				for(int i=0;i<array.length;i++)
				context.write(new Text(array[i].trim()), new Text(""));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			context.write(key, new Text(""));
				}
		}
		
	public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
	Configuration config = new Configuration();
	String[] otherArgs = new GenericOptionsParser(config,args).getRemainingArgs();
	@SuppressWarnings("deprecation")
	Job job = new Job (config, "Question 1");
	job.setJarByClass(HW1Question1.class);
	job.setMapperClass(map1.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);  
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
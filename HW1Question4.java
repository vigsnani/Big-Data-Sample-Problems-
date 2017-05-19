package JavaHDFS.JavaHDFS;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class HW1Question4{
	
	public static class map1 extends Mapper<LongWritable,Text,Text,Text>{
		
		HashSet<String> hashSet = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException{
	        Configuration conf = context.getConfiguration();
			String inputFile= conf.get("hdfs://cshadoop1/user/vxv160730/Question1/business.csv");
			Path filteredFilePath = new Path("hdfs://cshadoop1/user/vxv160730/Question1/business.csv");
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] filestatus = fs.listStatus(filteredFilePath);

			for (FileStatus status : filestatus) {
			Path pt = status.getPath();
	        BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;
	        line=br.readLine();
	        while (line != null){
	        	String data[]=line.split("::");
	        	if(data[1].contains("Stanford")&&data.length>2)
	        	{
	        		hashSet.add(data[0].trim());
	        	}
	            line=br.readLine();
	        	}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] data=value.toString().split("::");
				if(data.length>3&& hashSet.contains(data[2])){
				context.write(new Text(data[1]), new Text(data[3]));
				}
		}
	}
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		FileSystem fs = FileSystem.get(conf);
		Path outputFilePath = new Path(args[2]);
		if(fs.exists(outputFilePath)){
		fs.delete(outputFilePath,true);
		}

		conf.set("filterFileURL", otherArgs[1]);
		Job job = new Job(conf, "Question4");

		job.setJarByClass(HW1Question4.class);
		job.setMapperClass(map1.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);

		}
}
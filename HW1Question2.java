package JavaHDFS.JavaHDFS;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Map.Entry;

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

public class HW1Question2{
	
	public static class map1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data[] = value.toString().split("::");
		context.write(new Text(data[2]),new Text((data[3])));
		}
	}

	public static class reduce1 extends Reducer<Text, Text, Text, Text> {

		private TreeMap<String, Float> map = new TreeMap<String, Float>();
		private Stars stars = new Stars(map);
		private TreeMap<String, Float> descendingMap = new TreeMap<String, Float>(stars);

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			for (Text value : values) {
				sum += Float.parseFloat(value.toString());
				count++;
			}

			float avg = new Float(sum / count);
			map.put(key.toString(), avg);
		}

		class Stars implements Comparator<String> {
			
			TreeMap<String, Float> map;
			public Stars(TreeMap<String, Float> Map) {
				this.map = Map;
			}

			public int compare(String a, String b) {
				if (map.get(a) >= map.get(b)) {
					return -1;
				} else {
					return 1;
				}
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {

			descendingMap.putAll(map);
			int count = 0;
			for (Entry<String, Float> t : descendingMap.entrySet()) {
				if (count == 10) {
					break;
				}
				context.write(new Text(t.getKey()),new Text(String.valueOf(t.getValue())));
				count++;
			}
		}
	}
	public static void main(String[] args) throws Exception{

		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		@SuppressWarnings("deprecation")
		Job job = new Job(config, "Question2"); 
		job.setJarByClass(Test.class);
		job.setMapperClass(map1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(reduce1.class);
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(Text.class); 

		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 

		System.exit(job.waitForCompletion(true) ? 0 : 1); 
		}
}
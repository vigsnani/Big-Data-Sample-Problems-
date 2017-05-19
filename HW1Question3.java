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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class HW1Question3{
	
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
	public static class map3 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] data = line.split("\t");
			context.write(new Text(data[0].trim()), new Text("1" + data[0].trim() + "::" + data[1].trim()));
		}

	}
	public static class map2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String data[] = value.toString().split("::");
			context.write(new Text(data[0].trim()), new Text("2"+ data[0].trim() + "::" + data[1].trim()+ "::" + data[2].trim()));
		}
	}
	public static class reduce2 extends Reducer<Text, Text, Text, Text> {
        private ArrayList<String> list = new ArrayList<String>();
        private ArrayList<String> list1 = new ArrayList<String>();
        private static String split = "\\::";
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
        	for (Text t : values) {
        		String data = t.toString();

				if (data.startsWith("1")) {
					list.add(data.substring(1));
				} else {
					list1.add(data.substring(1));
				}
			}
		}

		protected void cleanup(Context context) throws IOException,	InterruptedException {

			for (String data : list) {

				for (String data1 : list1) {

					String[] t1Split = data.split(split);
					String t1BusinessId = t1Split[0].trim();
					String[] t2Split = data1.split(split);
					String t2BusinessId = t2Split[0].trim();
					
					if (t1BusinessId.equals(t2BusinessId)) {

						context.write(new Text(t1BusinessId), new Text(t2Split[1] + "\t" + t2Split[2] + "\t"+ t1Split[1]));
						break;
					}

				}

			}

		}

	}
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {

		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		Job job1 = Job.getInstance(config, "JOB1");
		job1.setJarByClass(HW1Question3.class);
		job1.setMapperClass(map1.class);
		job1.setReducerClass(reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		
		boolean x = job1.waitForCompletion(true);
		if (x) {
			Configuration config2 = new Configuration();

			Job job2 = Job.getInstance(config2, "JOB2");
			job2.setJarByClass(HW1Question3.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]),TextInputFormat.class, map3.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, map2.class);

			job2.setReducerClass(reduce2.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.waitForCompletion(true);
		}
	}
}
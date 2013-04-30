package e185.assign08;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CitationGraph extends Configured implements Tool {
	
    public static class MapClass extends Mapper<Text, Text, IntWritable, IntWritable> {
      public static final int bucketSize = 20;
    	private IntWritable citationBucket = new IntWritable();
    	private IntWritable citationCount = new IntWritable();
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {
            int bucket = Integer.parseInt(key.toString()) / bucketSize; // Integer division
            int startNum = bucket* (bucketSize) + 1;
            
            citationBucket.set(startNum);
            citationCount.set(Integer.parseInt(value.toString()));
            		
        	context.write(citationBucket, citationCount);
        }
    }   
    public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {
        public static final int bucketSize = 20;
        private Text bar = new Text();
        private Text range = new Text();
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            
        	// Creating the range string
        	int startBucket = key.get();
        	range.set(startBucket + "-" + (startBucket + bucketSize - 1));
        	
        	// Calculating the number of stars
            int count = 0;
            for (IntWritable value : values) {
                count+= value.get();
            }
            double logCount = Math.log10( (double) count);
            int stars = (int) Math.ceil(logCount);
            String histBars = new String(new char[stars]).replace("\0",  "*");
            bar.set(histBars);
            
            context.write(range, bar);
        }
    }   
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
    
        Job job = new Job(conf);
        job.setJarByClass(CitationGraph.class);
        job.setJobName("Citation Graph");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);   
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
        System.exit(job.waitForCompletion(true)?0:1);    
        return 0;
    }  
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new CitationGraph(), args);       
        System.exit(res);
    }
}

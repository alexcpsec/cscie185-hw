package e185.assign08;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Math;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalculatePi extends Configured implements Tool {
	
	private static int NUMPOINTS = 100000;
	private static int nMappers = 100;
	
    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
 
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
        	
        	int countCircle = 0;
        	int numPoints = NUMPOINTS/nMappers;
        	Random gen = new Random();
        	
        	for (int i =0; i < numPoints; i++) {
        		// Generating random numbers from -1 to 1
        		double xcoord = (-1) + gen.nextDouble() * 2;
        		double ycoord = (-1) + gen.nextDouble() * 2;
        		// Calculating if it falls in the circle
        		if (Math.sqrt((xcoord * xcoord) + (ycoord * ycoord)) < 1) {
        			countCircle++;
        		}
        		
        	}	
        	context.write(new IntWritable(numPoints), new IntWritable(countCircle));
        }
    }   
    public static class Reduce extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {            

        	// Calculating the number of points in all circles
            int countCircle = 0;
            for (IntWritable value : values) {
                countCircle+= value.get();
            }
            
            double pi = 4.0 * (double) countCircle / (double) NUMPOINTS;
            
            context.write(new Text("PI:"), new DoubleWritable(pi));
        }
    }   
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
    
        Job job = new Job(conf);
        job.setJarByClass(CalculatePi.class);
        job.setJobName("Calculate Pi");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);   
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); 
        System.exit(job.waitForCompletion(true)?0:1);    
        return 0;
    }  
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new CalculatePi(), args);       
        System.exit(res);
    }
}

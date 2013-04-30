package e185.assign08;
import java.io.IOException;
import java.lang.InterruptedException;

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



public class InverterCounterNewAPI extends Configured implements Tool {
    
    public static class MapClass extends Mapper<Text, Text, Text, Text> {
        
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {
            
        	context.write(value, key);
        }
    }   
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
                           
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new IntWritable(count));
        }
    }   
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        
        Job job = new Job(conf);
        job.setJarByClass(InverterCounterNewAPI.class);
        job.setJobName("Inverter Counter");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);   
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
        System.exit(job.waitForCompletion(true)?0:1);    
        return 0;
    }  
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new InverterCounterNewAPI(), args);       
        System.exit(res);
    }
}

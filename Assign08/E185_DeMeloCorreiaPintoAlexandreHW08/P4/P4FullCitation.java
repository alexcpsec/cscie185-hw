package e185.assign08;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class P4FullCitation extends Configured    implements Tool {
	
    public static class CounterMapClass extends Mapper<Text, Text, Text, Text> {
        
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {
            
        	context.write(value, key);
        }
    }   
    public static class CounterReduce extends Reducer<Text, Text, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
                           
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new IntWritable(count));
        }
    }   
	
	
    public static class HistogramMapClass extends  Mapper<Text, Text, IntWritable, IntWritable> { 
    	
      private final static IntWritable uno = new IntWritable(1);
      private IntWritable citationCount = new IntWritable();   
      
      public void map(Text key, Text value, Context context) 
        throws IOException, InterruptedException {                 
          citationCount.set(Integer.parseInt(value.toString()));
          context.write(citationCount, uno);
        }
    }
    
   public static class HistogramReduce extends Reducer <IntWritable,IntWritable,IntWritable,IntWritable>  {    //public void reduce(IntWritable key, Iterator<IntWritable>values,
    	 public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
             throws IOException, InterruptedException {

          int count = 0;
          for (IntWritable val:values){   // Iterable allows
            	count += val.get();        // for looping
          }
          context.write(key, new IntWritable(count));
       }
   }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        FileSystem fs = FileSystem.get(conf);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path intermediate = new Path("Assign08/inter-P4");
        
        Job jobCounter = new Job(conf, "P4-InverterCounter");
        jobCounter.setJarByClass(P4FullCitation.class);
        FileInputFormat.setInputPaths(jobCounter, in);
        FileOutputFormat.setOutputPath(jobCounter, intermediate);
        jobCounter.setJobName("P4-InverterCounter");
        jobCounter.setMapperClass(CounterMapClass.class);
        jobCounter.setReducerClass(CounterReduce.class); 
        jobCounter.setInputFormatClass(KeyValueTextInputFormat.class);
        jobCounter.setOutputFormatClass(TextOutputFormat.class);
        jobCounter.setOutputKeyClass(Text.class);
        jobCounter.setOutputValueClass(Text.class);          
        
        jobCounter.waitForCompletion(true);
        
        conf.unset("mapreduce.input.keyvaluelinerecordreader.key.value.separator");
        Job jobHistogram = new Job(conf, "P4-CitationHistogram");
        jobHistogram.setJarByClass(P4FullCitation.class);
        FileInputFormat.setInputPaths(jobHistogram, intermediate);
        FileOutputFormat.setOutputPath(jobHistogram, out);
        jobHistogram.setJobName("P4-CitationHistogram");
        jobHistogram.setMapperClass(HistogramMapClass.class);
        jobHistogram.setReducerClass(HistogramReduce.class); 
        jobHistogram.setInputFormatClass(KeyValueTextInputFormat.class);
        jobHistogram.setOutputFormatClass(TextOutputFormat.class);
        jobHistogram.setOutputKeyClass(IntWritable.class);
        jobHistogram.setOutputValueClass(IntWritable.class);  
        
        jobHistogram.waitForCompletion(true);
        fs.delete(intermediate, true);
        
        System.exit(0);
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), 
                      new P4FullCitation(), args);
        System.exit(res);
    }
}

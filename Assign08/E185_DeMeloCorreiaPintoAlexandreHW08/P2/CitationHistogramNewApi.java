package edu.hu.bgd;
import java.io.IOException;
//import java.util.Iterator;
import java.lang.InterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CitationHistogramNewApi extends Configured    implements Tool {
    public static class MapClass extends  Mapper<Text, Text, 		      IntWritable, IntWritable> {      
      private final static IntWritable uno = new IntWritable(1);
      private IntWritable citationCount = new IntWritable();   
      public void map(Text key, Text value, Context context) 
        throws IOException, InterruptedException {                 
          citationCount.set(Integer.parseInt(value.toString()));
          context.write(citationCount, uno);
        }
    }
    
   public static class Reduce extends Reducer  	<IntWritable,IntWritable,IntWritable,IntWritable>  {    //public void reduce(IntWritable key, Iterator<IntWritable>values,
    	 public void reduce(IntWritable key, Iterable<IntWritable>   	          values, Context context) 
             throws IOException, InterruptedException {
          int count = 0;
          // while (values.hasNext()) {
          //     count += values.next().get();
          for (IntWritable val:values){   // Iterable allows
            	count += val.get();        // for looping
          }
          context.write(key, new IntWritable(count));
       }
   }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //JobConf job = new JobConf(conf, CitationHistogram);
        //job.setJobName("CitationHistogram");
        Job job = new Job(conf, "CitationHistogram");
        job.setJarByClass(CitationHistogramNewApi.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);   
        job.setJobName("CitationHistogramNewApi");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class); 
       // job.setInputFormat(KeyValueTextInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //job.setOutputFormat(TextOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);  
        //JobClient.runJob(job);
        System.exit(job.waitForCompletion(true)?0:1);  
        return 0;
    }
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), 
                      new CitationHistogramNewApi(), args);
        System.exit(res);
    }
}

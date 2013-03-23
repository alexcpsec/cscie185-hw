package e185.assign06;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FlipWordFreq {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable freq = new IntWritable();
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // P3: Expecting to receive a line with the word and the frequency separated by whitespace
      StringTokenizer itr = new StringTokenizer(value.toString());
      int intFreq = 0;
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        intFreq = Integer.parseInt(itr.nextToken());
        freq.set(intFreq);
        context.write(word, freq);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum = 0;
			
      for (IntWritable val : values) {
        sum += val.get();
      }
			result.set(sum);
      
      context.write(key, result);
    }
  }

  public static class WordFreqReducer 
       extends Reducer<Text, IntWritable, IntWritable, Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum = 0;
			
      // P3: Do not expect to have more than one int, but I will iterate on the collection anyway
      for (IntWritable val : values) {
        sum += val.get();
      }
			result.set(sum);
      
      // P3: Inverting the output
      context.write(result, key);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: FlipWordFreq <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "flip word freq");
    job.setJarByClass(FlipWordFreq.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(WordFreqReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

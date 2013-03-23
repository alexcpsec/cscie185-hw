package e185.assign06;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

public class SimpleWordCount {

  public static void main(String[] args) throws Exception {
    
    JobClient client = new JobClient();
    JobConf conf = new JobConf(SimpleWordCount.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    conf.setMapperClass(TokenCountMapper.class);
    conf.setCombinerClass(LongSumReducer.class);
    conf.setReducerClass(LongSumReducer.class);
    client.setConf(conf);
    
    try {
      JobClient.runJob(conf);
    } catch (Exception e) { e.printStackTrace(); }
  
  }
}

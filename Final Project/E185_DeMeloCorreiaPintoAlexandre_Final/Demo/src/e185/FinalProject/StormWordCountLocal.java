package e185.FinalProject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.spout.SchemeAsMultiScheme;

import java.util.List;
import java.util.StringTokenizer;
import java.util.ArrayList;

import storm.kafka.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */

public class StormWordCountLocal {
	
    public static class SplitSentence extends BaseBasicBolt {
    	 
    	 @Override
         public void execute(Tuple tuple, BasicOutputCollector collector) {
    		 String sentence = tuple.getString(0);
    	     StringTokenizer itr = new StringTokenizer(sentence, " ");
    	     while (itr.hasMoreTokens()) {
    	    	String word = itr.nextToken();
    	    	collector.emit(new Values(word, 1));
    	     }
    		 
    	 }
    	 
         @Override
         public void declareOutputFields(OutputFieldsDeclarer declarer) {
             declarer.declare(new Fields("word", "count"));
         }
    	 
    }
    
    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if(count==null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    public static class ConsolePrinter extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = tuple.getInteger(1);
            System.out.println(word + " -- " + count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            
        }
    }
    
    public static class LocalFilePerisitence extends BaseBasicBolt {
        
    	String _outputDir;
    	File _outputFile;
    	Writer _w;

        public LocalFilePerisitence(String outputDir) {
        	_outputDir = outputDir;
        	File dir = new File(_outputDir);
        	if (!dir.exists()) dir.mkdirs();
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context) {
            int myId = context.getThisTaskId();
            _outputFile = new File(_outputDir + "/output_" + myId);
            FileOutputStream is;
			try {
				is = new FileOutputStream(_outputFile);
				OutputStreamWriter osw = new OutputStreamWriter(is);    
	            _w = new BufferedWriter(osw);
			} catch (FileNotFoundException e) {
				System.out.println("Unable to open " + _outputFile);
				e.printStackTrace();
			}
            
        }
        
        
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = tuple.getInteger(1);
            try {
				_w.write(word + "\t" + count + "\n");
				_w.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
            
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            
        }
    }
    
    public static void main(String[] args) throws Exception {
        
		System.out.println("e185 - Final Project");
		System.out.println("Alexandre de Melo Correia Pinto");
		System.out.println("Storm WordCount Local Class - connects to local Zookeeper, " +  
			"attaches to Kafka topic and performs wordcount on the messages, " + 
			"outputing incremental counts to a local directory");
    	
    	if(args!=null && args.length >= 2) {
    	
    		String topic = args[0];
    		String outputDir = args[1];
    		
    		// Creating the Kafka Spout to point to the specific topic
	    	List<String> hosts = new ArrayList<String>();
	        hosts.add("localhost");
	        
	        SpoutConfig spoutConfig = new SpoutConfig(
	        		  KafkaConfig.StaticHosts.fromHostString(hosts, 1),
	        		  topic, // topic to read from
	        		  "/kafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
	        		  "discovery"); // an id for this consumer for storing the consumer offsets in Zookeeper
	        
	        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        spoutConfig.forceStartOffsetTime(-2);
	        
	        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	        
	        // Building the topology for Storm
	        TopologyBuilder builder = new TopologyBuilder();
	                
	        builder.setSpout("kafka", kafkaSpout, 5);
	        
	        builder.setBolt("split", new SplitSentence(), 10)
	                 .shuffleGrouping("kafka");
	        builder.setBolt("count", new WordCount(), 20)
	                 .fieldsGrouping("split", new Fields("word"));
	        builder.setBolt("print", new ConsolePrinter(), 5)
	        		 .shuffleGrouping("count");
	        builder.setBolt("output", new LocalFilePerisitence(outputDir), 5)
   		 			.shuffleGrouping("count");
	                
	        Config conf = new Config();              
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

    	} else {
            System.out.println("usage: e185.FinalProject.StormWordCountLocal topic outputDir");
            System.out.println("topic - The Kafka topic to be connected to");
            System.out.println("outputDir - The output directory for the incremental wordcounts");
      	}
    }

}

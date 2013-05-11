package e185.FinalProject;

import java.io.File;
import java.io.IOException;
import java.util.*;
import kafka.javaapi.producer.*;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.FileUtils;


public class KafkaFileStreamer {
	
	public static Producer<String, String> initProducer() {
		Properties props = new Properties();
		props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		return producer;
	}

	public static void sendMessage(Producer<String, String> producer, String topic, List<String> value) {
		ProducerData<String, String> data = new ProducerData<String, String>(topic, value);
		producer.send(data);
	}

	public static void main(String[] args) throws IOException {

		System.out.println("e185 - Final Project");
		System.out.println("Alexandre de Melo Correia Pinto");
		System.out.println("Kafka File Streamer Class - sends the content of a text file through a Kafka Server that has been registered on this machine's Zookeeper\n");
		
		if(args!=null && args.length > 1) {

			String topic = args[0];
            String fileName = args[1];
            int times = Integer.parseInt(args[2]); 
			
			Producer<String, String> producer = initProducer();

            List<String> lines = FileUtils.readLines(new File(fileName));
            
            for (int i = 0; i<times; i++) {
            	System.out.println("Sending " + fileName + " to Kafka system. Iteration: " + (i+1));
            	sendMessage(producer, topic, lines);
            }
            
            System.out.println("Messgaes sent!");
            producer.close();
            
        } else {        
            System.out.println("usage: e185.FinalProject.KafkaFileStreamer topic filename repetitions");
            System.out.println("topic - The topic to be used on the Kafka system to send the files");
            System.out.println("filename - file name of the text file to be sent through Kafka");
            System.out.println("repetitions - number of times to send the file thorugh Kafka");
        }	
	}
}

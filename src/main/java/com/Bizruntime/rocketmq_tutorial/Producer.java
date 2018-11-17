package com.Bizruntime.rocketmq_tutorial;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {
	
	public static void main(String[] args) throws MQClientException, InterruptedException { 
		
		 
	          DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName"); 
		  
	          producer.setNamesrvAddr("localhost:9876");
		 
		          producer.start(); 
		  
		 
		          for (int i = 0; i < 20; i++) 
		              try { 
		                  { 
		                      Message msg = new Message("TopicTest", 
		                          "TagA", 
		                          "OrderID188", 
		                          "Hello @Krishna Successful run!".getBytes(RemotingHelper.DEFAULT_CHARSET)); 
		                      SendResult sendResult = producer.send(msg); 
		                      System.out.printf("%s%n", sendResult); 
		                  } 
		  
		 
		              } catch (Exception e) { 
		                  e.printStackTrace(); 
		              } 
		  
		 
		          producer.shutdown(); 
} 


}

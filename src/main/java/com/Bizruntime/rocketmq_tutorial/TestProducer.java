package com.Bizruntime.rocketmq_tutorial;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TestProducer {

	public static void main(String[] args) throws MQClientException, InterruptedException { 
		          DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName"); 
		          
		          producer.setNamesrvAddr("localhost:9876");
		          
		          producer.start(); 
		  
		 
		          for (int i = 0; i < 2; i++) 
		              try { 
		                  { 
		                      Message msg = new Message("TopicTest", 
		                          "TagA", 
		                          "key113", 
		                          "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET)); 
		                      SendResult sendResult = producer.send(msg); 
		                      System.out.printf("%s%n", sendResult); 
		  
		 
		                      QueryResult queryMessage = 
		                          producer.queryMessage("TopicTest", "key113", 10, 0, System.currentTimeMillis()); 
		                      for (MessageExt m : queryMessage.getMessageList()) { 
		                          System.out.printf("%s%n", m); 
		                      } 
		                  } 
		  
		 
		              } catch (Exception e) { 
		                  e.printStackTrace(); 
		              } 
		          producer.shutdown(); 
		     } 

}

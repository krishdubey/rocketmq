package com.Bizruntime.rocketmq_tutorial;

 import java.io.UnsupportedEncodingException; 
 import java.util.concurrent.CountDownLatch; 
 import java.util.concurrent.TimeUnit; 
 import org.apache.rocketmq.client.exception.MQClientException; 
 import org.apache.rocketmq.client.producer.DefaultMQProducer; 
 import org.apache.rocketmq.client.producer.SendCallback; 
 import org.apache.rocketmq.client.producer.SendResult; 
 import org.apache.rocketmq.common.message.Message; 
 import org.apache.rocketmq.remoting.common.RemotingHelper; 

public class AsyncProducer {
	
	public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException { 
			
			 
			         DefaultMQProducer producer = new DefaultMQProducer("Jodie_Daily_test"); 
			         
			         producer.setNamesrvAddr("localhost:9876");
			          producer.start(); 
			          producer.setRetryTimesWhenSendAsyncFailed(0); 
			  
			 
			          int messageCount = 10; 
			          final CountDownLatch countDownLatch = new CountDownLatch(messageCount); 
			          //CountDownLatch is a concurrency construct that allows one or more threads to wait for a given set of operations to complete
			          for (int i = 0; i < messageCount; i++) { 
			              try { 
			                  final int index = i; 
			                  Message msg = new Message("TopicTest", 
			                      "TagA", 
			                      "OrderID188", 
			                      "Hello krishna!".getBytes(RemotingHelper.DEFAULT_CHARSET)); 
			                  producer.send(msg, new SendCallback() { 
			                    
			                	  
			                	  //@Override 
			                      public void onSuccess(SendResult sendResult) { 
			                          countDownLatch.countDown(); 
			                          System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId()); 
			                      } 
			  
			 
			                     // @Override 
			                      public void onException(Throwable e) { 
			                          countDownLatch.countDown(); 
			                          System.out.printf("%-10d Exception %s %n", index, e); 
			                          e.printStackTrace(); 
			                      } 
			                  }); 
			              } catch (Exception e) { 
			                  e.printStackTrace(); 
			              } 
			          } 
			          countDownLatch.await(5, TimeUnit.SECONDS); 
			          producer.shutdown(); 
			      } 


}

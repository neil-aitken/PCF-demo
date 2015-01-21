package com.pivotal.example.xd;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ErrorHandler;

import com.pivotal.example.xd.controller.OrderController;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitClient {

	static Logger logger = Logger.getLogger(RabbitClient.class);
	private static RabbitClient instance;
	private CachingConnectionFactory ccf;
	private Queue orderQueue;
	private Queue orderProcQueue;
	private RabbitTemplate rabbitTemplate;
	
	private static final String EXCHANGE_NAME="ORDERS_EXCHANGE";
	private static final String ORDER_PROCESSING_QUEUE = "ORDERS_QUEUE";

	Connection connection;
	private String rabbitURI;
	
	private RabbitClient(){
		try{
	    				
	    				ConnectionFactory factory = new ConnectionFactory();
	    				factory.setThreadFactory(com.google.appengine.api.ThreadManager.currentRequestThreadFactory());
	    				factory.setRequestedHeartbeat(5);
	    				rabbitURI = "amqp://admin:cloud@146.148.60.247:5672";
	    				factory.setUri(rabbitURI);
	    				ccf = new CachingConnectionFactory(factory);
	    				
	    				connection = ccf.createConnection();
	    				
	    				FanoutExchange fanoutExchange = new FanoutExchange(EXCHANGE_NAME, false, true);
	    				
	    				RabbitAdmin rabbitAdmin = new RabbitAdmin(ccf);
	    				
	    				rabbitAdmin.declareExchange(fanoutExchange);
	    				
	    				orderQueue = new AnonymousQueue();
	    				rabbitAdmin.declareQueue(orderQueue);
	    				rabbitAdmin.declareBinding(BindingBuilder.bind(orderQueue).to(fanoutExchange));
	    				
	    				orderProcQueue = new Queue(ORDER_PROCESSING_QUEUE);
	    				rabbitAdmin.declareQueue(orderProcQueue);
	    				rabbitAdmin.declareBinding(BindingBuilder.bind(orderProcQueue).to(fanoutExchange));
	    				
	    				
	    				rabbitTemplate = rabbitAdmin.getRabbitTemplate();
	    				rabbitTemplate.setExchange(EXCHANGE_NAME);
	    				rabbitTemplate.setConnectionFactory(ccf);
	    				
	    				rabbitTemplate.afterPropertiesSet();
	    					    				
	    			}
	    			catch(Exception e){
	    				logger.error(e);
	    				throw new RuntimeException("Exception connecting to RabbitMQ",e);
	    			}
		
		
	}
	
	public static void main(String args[]) throws Exception{
		RabbitClient client = RabbitClient.getInstance();
		client.post(new Order());
	}
	
	public static synchronized RabbitClient getInstance(){
		if (instance==null){
			instance = new RabbitClient(); 
		}
		return instance;
	}
	
	public synchronized void post(Order order) throws IOException{
		
		rabbitTemplate.send(new Message(order.toBytes(), new MessageProperties()));
		logger.info("Sent message " + order.toString());
	}

	public void startMessageListener(){
		
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(ccf);		
		container.setTaskExecutor(new SimpleAsyncTaskExecutor(com.google.appengine.api.ThreadManager.currentRequestThreadFactory()));
		container.setQueues(orderQueue);
		container.setErrorHandler(new ErrorHandler() {
			
			@Override
			public void handleError(Throwable t) {
				logger.error(t);
				
			}
		});
		container.setMessageListener(new MessageListener() {
			
			@Override
			public void onMessage(Message message) {
				logger.info("Processing message " + message.toString());
				OrderController.registerOrder(Order.fromBytes(message.getBody()));
			}
		});
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.start();
		logger.info("Is running: " +container.isRunning());
		
	}

	public void startOrderProcessing(){
		
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(ccf);		
		container.setTaskExecutor(new SimpleAsyncTaskExecutor(com.google.appengine.api.ThreadManager.currentRequestThreadFactory()));
		
		container.setQueues(orderProcQueue);
		container.setErrorHandler(new ErrorHandler() {
			
			@Override
			public void handleError(Throwable t) {
				logger.error(t);
				
			}
		});
		container.setMessageListener(new MessageListener() {
			
			@Override
			public void onMessage(Message message) {
				//for now simply log the order
				Order order = Order.fromBytes(message.getBody());
				logger.info("Process Order: " + order.getState()+":"+order.getAmount());
			}
		});
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.start();
		logger.info("Is running order processing: " +container.isRunning());
		
	}

	
	
	public boolean isBound(){
		return (rabbitURI!=null);
	}
	
	public String getRabbitURI(){
		return rabbitURI;
	}
}

package com.meteor.module;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Sender {
	
	String host;
	String basic_queue_name = "basicQ";
	
	String durable_queue_name = "durableQ";
	
	
	String fanout_queue_name = "fanout_durableQ";
	
	
	/*
	channel.basicPublish("", durable_queue_name, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
	MessageProperties.PERSISTENT_TEXT_PLAIN
	durable + PERSISTENT_TEXT_PLAIN 일 경우에만 RabbitMQ 다운의 경우에도 백업
	durable 만 있는 경우, queue는 남아 있지만 데이터는 서버 다운 시 데이터는 소멸 된다. 
	PERSISTENT_TEXT_PLAIN만 있는 경우 queue 가 통채로 날아간다.
	*/
	
	
	boolean durable = true;
	/*
	When RabbitMQ quits or crashes it will forget the queues 
	and messages unless you tell it not to. 
	Two things are required to make sure that messages aren't lost: 
	we need to mark both the queue and messages as durable.
	*/
		
	public Sender(String host){
		set_host(host);
	}
	
	public void set_host(String addr){
		this.host = addr;
		
	}
	
	public void basic_send(String tex){
		
		
		String message;
		message = tex;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		Connection conn;
		try {
			conn = cf.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(basic_queue_name,durable,false,false,null);
			channel.basicPublish("", basic_queue_name, null, message.getBytes());

			System.out.println("send : " + message);
			
			channel.close();
			conn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void durable_send(String tex){
		
		
		String message;
		message = tex;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		Connection conn;
		try {
			conn = cf.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(durable_queue_name,durable,false,false,null);
			channel.basicPublish("", durable_queue_name, null, message.getBytes());

			System.out.println("send : " + message);
			
			channel.close();
			conn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void fanout_durable_send(String tex){
		
		
		String message;
		message = tex;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		Connection conn;
		try {
			conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			channel.exchangeDeclare("logs", "fanout");
			
			/*
			channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			//channel.basicPublish("", durable_queue_name, null, message.getBytes());
			*/
			/*
			channel.basicPublish("", fanout_queue_name,
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			*/
			/*
			channel.basicPublish("", fanout_queue_name,
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			*/
			/*
			//queueDeclare
			channel.queueDeclare(fanout_queue_name+"a",durable,false,false,null);
			channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			
			//bind
			channel.queueBind(fanout_queue_name+"a", "logs", "");
			channel.queueBind(fanout_queue_name, "logs", "");
			*/
			
			
			//exchange input
			channel.basicPublish("logs", "",
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			
			//MessageProperties.PERSISTENT_TEXT_PLAIN
			
			System.out.println("send : " + message);
			
			channel.close();
			conn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
public void direct_durable_send(String tex){
		
		
		String message;
		message = tex;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		Connection conn;
		try {
			conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			channel.exchangeDeclare("di_logs", "direct");
			//channel.exchangeDeclare("logs", "fanout");
			
			/*
			channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			//channel.basicPublish("", durable_queue_name, null, message.getBytes());
			*/
			/*
			channel.basicPublish("", fanout_queue_name,
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			*/
			/*
			channel.basicPublish("", fanout_queue_name,
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			*/
			/*
			//queueDeclare
			channel.queueDeclare(fanout_queue_name+"a",durable,false,false,null);
			channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			
			//bind
			channel.queueBind(fanout_queue_name+"a", "logs", "");
			channel.queueBind(fanout_queue_name, "logs", "");
			*/
			
			
			//exchange input
			channel.basicPublish("di_logs", "critical",
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			
			//MessageProperties.PERSISTENT_TEXT_PLAIN
			
			System.out.println("send : " + message);
			
			channel.close();
			conn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
public void topic_durable_send(String tex){
	
	
	String message;
	message = tex;
	ConnectionFactory cf = new ConnectionFactory();
	cf.setHost(host);
	Connection conn;
	try {
		conn = cf.newConnection();
		Channel channel = conn.createChannel();
		
		channel.exchangeDeclare("topic_logs", "topic");
		
		
		
		//exchange input
		//channel.basicPublish("topic_logs", "critical",
		//channel.basicPublish("topic_logs", "cr.itical",
		channel.basicPublish("topic_logs", "kim.i.2",
				MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
		
		/*
		direct 와 차이점은 정확히 일치 하지 않아도
		*(one) 이나 
		#(zero or more) 로
		분류 해서 사용할수 있다는거 뿐인듯..		
		*/
		//MessageProperties.PERSISTENT_TEXT_PLAIN
		
		System.out.println("send : " + message);
		
		channel.close();
		conn.close();
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}

}

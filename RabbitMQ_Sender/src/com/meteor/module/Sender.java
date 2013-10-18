package com.meteor.module;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Sender {
	
	String host;
	String basic_queue_name = "basicQ";
	String durable_queue_name = "durableQ";
	
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


}

package com.meteor.module;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Sender {

	public Sender(String host){
		set_host(host);
	}
	
	String host;
	String basic_queue_name = "basicQ";
	
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
			channel.queueDeclare(basic_queue_name,false,false,false,null);
			channel.basicPublish("", basic_queue_name, null, message.getBytes());

			System.out.println("send : " + message);
			
			channel.close();
			conn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
	}
}

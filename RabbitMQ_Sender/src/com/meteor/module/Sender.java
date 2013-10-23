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
	durable + PERSISTENT_TEXT_PLAIN �� ��쿡�� RabbitMQ �ٿ��� ��쿡�� ���
	durable �� �ִ� ���, queue�� ���� ������ �����ʹ� ���� �ٿ� �� �����ʹ� �Ҹ� �ȴ�. 
	PERSISTENT_TEXT_PLAIN�� �ִ� ��� queue �� ��ä�� ���ư���.
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
		direct �� �������� ��Ȯ�� ��ġ ���� �ʾƵ�
		*(one) �̳� 
		#(zero or more) ��
		�з� �ؼ� ����Ҽ� �ִٴ°� ���ε�..		
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

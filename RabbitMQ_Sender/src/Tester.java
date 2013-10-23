import com.meteor.module.Sender;


public class Tester {

	public static void main(String[] args) {

		
		
		String host=null;
		

		
		Sender sen = new Sender(host);
		
		String tex = " fan";
		
		for(int f1=0;f1<1000;f1++){
			
			//sen.fanout_durable_send( f1 + tex );
			//sen.direct_durable_send(f1 + tex);
			sen.topic_durable_send(f1 + tex);
		}
		
		
		
		
		

	}

}

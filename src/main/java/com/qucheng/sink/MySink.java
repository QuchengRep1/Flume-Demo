package com.qucheng.sink;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;

public class MySink extends AbstractSink implements Configurable {
	
	private Logger logger = org.slf4j.LoggerFactory.getLogger(MySink.class);
	
	private String prefix;
	private String subfix;
	
	public Status process() throws EventDeliveryException {
		
		Status status = null;
		
		Channel channel = getChannel();
		
		Transaction transaction = channel.getTransaction();
		
		transaction.begin();

		try {
			Event event = channel.take();
			
			if (event != null) {
				String body = new String(event.getBody());
				logger.info( System.currentTimeMillis() + prefix + body + subfix + new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date()));
			}
			
			transaction.commit();
			
			status = Status.READY;
			
		} catch (ChannelException e) {
			e.printStackTrace();
			
			transaction.rollback();
			
			status = Status.BACKOFF;
		} finally {
			transaction.close();
		}
		return status;
	}

	public void configure(Context context) {

		prefix = context.getString("prefix");
		subfix = context.getString("subfix", "20201001");
	}

}

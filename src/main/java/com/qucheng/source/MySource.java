package com.qucheng.source;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable,PollableSource {
	
	private String prefix;
	private String subfix;
	
	public Status process() throws EventDeliveryException {
		
		Status status = null;
		
		try {
			for (int i = 0; i < 5; i++) {
				SimpleEvent event = new SimpleEvent();
				event.setBody((prefix + "--" + i + "--" + subfix).getBytes());

				getChannelProcessor().processEvent(event);
				
				status = Status.READY;
			}
		} catch (Exception e) {

			e.printStackTrace();
			status = Status.BACKOFF;
		}
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return status;
	}

	public long getBackOffSleepIncrement() {
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		return 0;
	}

	public void configure(Context context) {
		prefix = context.getString("prefix");
		subfix = context.getString("subfix", "20201001");
	}
	
}

package com.qucheng.interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class TypeInterceptor implements Interceptor {
	
	private List<Event> headerEvents;

	public void initialize() {
		headerEvents = new ArrayList<>();
	}

	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		String body = new String(event.getBody());
		
		if (body.contains("bin")) {	
			headers.put("type", "uat-bin");
		}
		
		if (body.contains("rely")) {	
			headers.put("type", "uat-rely");
		}
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			headerEvents.add(intercept(event));
		}
		
		return headerEvents;
	}

	public void close() {
		
	}
	
	public static class Builder implements Interceptor.Builder {

		public void configure(Context context) {
			
		}

		public Interceptor build() {
			return new TypeInterceptor();
		}
		
	}

}


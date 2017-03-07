package com.semantix;

import java.util.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomHostInterceptor
        implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(CustomHostInterceptor.class);
    private String hostHeader;

    public CustomHostInterceptor(String hostHeader){
        this.hostHeader = hostHeader;
    }


    public void initialize() {
        // At interceptor start
    }

    public Event intercept(Event event) {
        byte[] json = event.getBody();
        LOG.info("JSON data: "  + new String(json));
        LOG.info("Event: " + event.toString());
        JSONObject object = new JSONObject(new String(json));
        Map<String,String> out = new HashMap<String, String>();

        //parse the json to map
        parse(object,out);

        //get the data
        String alarm_name = out.get("alarm_name");
        String object1 = out.get("object");
        String status = out.get("status");
        String startts = out.get("startts");
        String endts = out.get("endts");
        String urgency = out.get("urgency");

        //create the csv
        String jsonCsv = alarm_name + "," + object1 + "," + status + "," + startts + "," + endts + "," + urgency;

        //remove eventual '\n'
        String fixedCsv  = jsonCsv.replace("\n","");
        LOG.info("Csv data: "+ fixedCsv);

        event.setBody(fixedCsv.getBytes());

        return event;

    }


    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            // Intercept any event
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    public void close() {
        // At interceptor shutdown
    }

    public static class Builder
            implements Interceptor.Builder {

        private String hostHeader;

        public void configure(Context context) { }


        public Interceptor build() {
            return new CustomHostInterceptor(hostHeader);
        }
    }

    //JSON -> MAP. parse a json to map
    public static Map<String,String> parse(JSONObject json , Map<String,String> out) throws JSONException {
        Iterator<String> keys = json.keys();
        while(keys.hasNext()){
            String key = keys.next();
            String val = null;
            try{
                JSONObject value = json.getJSONObject(key);
                parse(value,out);
            }catch(Exception e){
                val = json.getString(key);
            }

            if(val != null){
                out.put(key,val);
            }
        }
        return out;
    }

}
package mecha.monitoring;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

public class Monitoring {
    final private static Logger log = 
        Logger.getLogger(Monitoring.class.getName());
    
    final private Map<String, Metric> metrics;
    final private SystemLog systemLog;
    final private Thread reporterThread;
    
    private class MetricsReporter implements Runnable {
        public void run() {
            while(true) {
                try {
                    dumpMetrics();
                    Thread.sleep(60000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    public Monitoring() throws Exception {
        metrics = new ConcurrentHashMap<String, Metric>();
        systemLog = new SystemLog();
        
        reporterThread = new Thread(new MetricsReporter());
        reporterThread.start();
    }
    
    public void start() throws Exception {
        systemLog.start();
    }
    
    /*
     * Add a value to a metric object by name and index
     *  that value in the system log.
    */
    public void metric(String name, double val) throws Exception {
        metrics.get(name).addValue(val);
        JSONObject doc = new JSONObject();
        doc.put("value_d", val);
        systemLog.logMetric(name, doc);
    }
    
    /*
     * Index a log entry.
    */
    public void log(String name, JSONObject msg) throws Exception {
        systemLog.log(name, msg);
    }
    
    /*
     * Get a metric object by full name.
    */
    public Metric getMetric(String name) throws Exception {
        return metrics.get(name);
    }
    
    /*
     * Remove a metric from metrics map and delete all metric 
     *  and log entries in the system index.
    */
    public void remove(String name) throws Exception {
        metrics.remove(name);
        systemLog.deleteByQuery("name:\"" + name + "\"");
    }
    
    /*
     * Create and initialize a new metric for use.
    */
    public Metric createMetric(String name, int windowSize) throws Exception {
        metrics.put(name, new Metric(name, windowSize));
        return metrics.get(name);
    }
    
    public SystemLog getLog() {
        return systemLog;
    }
    
    public void dumpMetrics() throws Exception {
        for(String name : metrics.keySet()) {
            log.info("METRIC: " + name);
            log.info(metrics.get(name).getStats().toString());
            log.info("");
        }
    }
}


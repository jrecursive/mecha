package mecha.monitoring;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

public class Monitoring {
    final private static Logger log = 
        Logger.getLogger(Monitoring.class.getName());
        
    /*
     * This equates to the number of seconds of history to
     *  keep for each metric (rolling).
    */
    final private static int DEFAULT_RATE_WINDOW_SIZE = 3600;
    
    final private Set<WeakReference<Rates>> rateMonitorables;
    final private Map<String, Metric> metrics;
    final private SystemLog systemLog;
    
    final private Thread reporterThread;
    final private Thread monitorThread;
    
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
    
    private class MonitorableRateLogger implements Runnable {
        public void run() {
            while(true) {
                try {
                    Set<WeakReference<Rates>> deadRefs = 
                        new HashSet<WeakReference<Rates>>();
                    for(WeakReference<Rates> ref : rateMonitorables) {
                        if (ref.get() == null) {
                            deadRefs.add(ref);
                            continue;
                        }
                        Rates rates = ref.get();
                        ConcurrentHashMap<String, AtomicInteger> rateMap =
                            rates.getRateMap();
                        for(String name : rateMap.keySet()) {
                            if (!metrics.containsKey(name)) {
                                createMetric(name, DEFAULT_RATE_WINDOW_SIZE);
                            }
                            metric(name, rateMap.get(name).doubleValue());
                        }
                        rates.clear();
                    }
                    for (WeakReference<Rates> deadRef : deadRefs) {
                        rateMonitorables.remove(deadRef);
                    }
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    public Monitoring() throws Exception {
        metrics = new ConcurrentHashMap<String, Metric>();
        systemLog = new SystemLog();
        rateMonitorables = new HashSet<WeakReference<Rates>>();
        
        reporterThread = new Thread(new MetricsReporter());
        reporterThread.start();
        
        monitorThread = new Thread(new MonitorableRateLogger());
        monitorThread.start();
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
    
    public void addMonitoredRates(Rates rateMonitorable) {
        rateMonitorables.add(new WeakReference<Rates>(rateMonitorable));
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


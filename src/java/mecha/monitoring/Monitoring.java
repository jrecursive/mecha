/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
    final private static int DEFAULT_RATE_WINDOW_SIZE = 300;
    
    final private Set<WeakReference<Rates>> rateMonitorables;
    final private Map<String, Metric> metrics;
    final private SystemLog systemLog;
    final private RiakMonitor riakMonitor;
    final private MechaMonitor mechaMonitor;
    
    final private Thread wranglerThread;
    final private Thread monitorThread;
    
    final private Rates rates;
    
    private class SolrLogWrangler implements Runnable {
        public void run() {
            while(true) {
                try {
                    tameSolrLogging();
                    Thread.sleep(10000);
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
                    rateMonitorables.removeAll(deadRefs);
                    Thread.sleep(5000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    public Monitoring() throws Exception {
        rates = new Rates();
        metrics = new ConcurrentHashMap<String, Metric>();
        systemLog = new SystemLog();
        rateMonitorables = new CopyOnWriteArraySet<WeakReference<Rates>>();
        
        wranglerThread = new Thread(new SolrLogWrangler());
        wranglerThread.start();
        
        monitorThread = new Thread(new MonitorableRateLogger());
        monitorThread.start();
        
        riakMonitor = new RiakMonitor();
        mechaMonitor = new MechaMonitor();
    }
    
    public void start() throws Exception {
        systemLog.start();
        riakMonitor.start();
        mechaMonitor.start();
        addMonitoredRates(rates);
    }
    
    public void stop() throws Exception {
        systemLog.stop();
        riakMonitor.stop();
        mechaMonitor.stop();
    }
    
    /*
     * Add a value to a metric object by name and index
     *  that value in the system log.
    */
    public void metric(String name, double val) throws Exception {
        /*
         * TODO: configurable window sizes by metric name.
        */
        if (!metrics.containsKey(name)) {
            createMetric(name, DEFAULT_RATE_WINDOW_SIZE);
        }
        metrics.get(name).addValue(val);

        /*
         * $metric.<name>
        */
        
        String channel = "$metric." + name;
        
        if (Mecha.getChannels().getChannel(channel) != null) {
            Mecha.getChannels()
                 .getChannel(channel)
                 .send(metrics.get(name).asJSON(1, false));
        }
        /*
        JSONObject doc = new JSONObject();
        doc.put("value_d", val);
        systemLog.logMetric(name, doc);
        */
    }
    
    /*
     * Index a log entry.
    */
    public void error(String name, Throwable t) {
        try {
            String message = t.getMessage();
            String shortMessage = t.toString();
            String traceId = Mecha.guid(t.getClass().getName());
            
            JSONObject logMsg = new JSONObject();
            logMsg.put("short_message_t", shortMessage);
            logMsg.put("trace_id_s", traceId);
            logMsg.put("is_error_b", true);
            systemLog.log(name, message, logMsg);
            
            int seq = 0;
            for(StackTraceElement el : t.getStackTrace()) {
                JSONObject msg = new JSONObject();
                msg.put("class_s", el.getClassName());
                msg.put("filename_s", el.getFileName());
                msg.put("method_s", el.getMethodName());
                msg.put("native_b", el.isNativeMethod());
                msg.put("line_number_i", el.getLineNumber());
                msg.put("trace_id_s", traceId);
                msg.put("seq_i", seq);
                systemLog.error(name, msg);
                seq++;
            }
            
            rates.add("mecha.global.exceptions");
        } catch (Exception ex) {
            /*
             * I truly hope this catch block does not become
             *  the source of profound unhappiness.
            */ 
            ex.printStackTrace();
        }
    }
    
    public void log(String name, String message, Logger logger) {
        if (message.trim().equals("")) return;
        logger.info(name + ": " + message);
        logData(name, message, null);
    }
    
    public void log(String name, String message) {
        logData(name, message, null);
    }
    
    public void logData(String name, String message, JSONObject data) {
        systemLog.log(name, message, data);
    }
    
    /*
     * Get a metric object by full name.
    */
    public Metric getMetric(String name) throws Exception {
        return metrics.get(name);
    }
    
    public JSONObject asJSON(int entries) throws Exception {
        return asJSON(entries, false);
    }
    
    public JSONObject asJSON(int entries, boolean summary) throws Exception {
        JSONObject obj = new JSONObject(); 
        for(String name : metrics.keySet()) {
            obj.put(name, metrics.get(name).asJSON(entries, summary));
        }
        return obj;
    }
    
    public Map<String, Metric> getMetrics() {
        return metrics;
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
    
    private void tameSolrLogging() throws Exception {
        Enumeration<String> loggers = LogManager.getLogManager().getLoggerNames();
        while(loggers.hasMoreElements()) {
            String loggerName = loggers.nextElement();
            if (loggerName.indexOf("solr") != -1) {
                Logger.getLogger(loggerName).setLevel(Level.WARNING);
            }
        }
    }
}


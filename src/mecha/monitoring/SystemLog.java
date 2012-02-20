package mecha.monitoring;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

public class SystemLog {
    final private static Logger log = 
        Logger.getLogger(SystemLog.class.getName());
    
    private SolrServer solrServer;
    final private LinkedBlockingQueue<SolrInputDocument> logDocQueue;
    final private Thread indexerThread;
    final private Thread prunerThread;
    
    public SystemLog() throws Exception {
        logDocQueue = new LinkedBlockingQueue<SolrInputDocument>();
        indexerThread = new Thread(new LogQueueIndexer());
        prunerThread = new Thread(new LogPruner());
    }
    
    protected void start() throws Exception {
        solrServer = Mecha.getSolrManager().getCore("system").getServer();
        
        log.info("Starting system log indexer thread...");
        indexerThread.start();
        
        log.info("Starting system log pruner thread...");
        prunerThread.start();
    }
 
    
    /*
     * Indexes batches of system log and metric entries.
    */
    private class LogQueueIndexer implements Runnable {
        public void run() {
            while(true) {
                try {
                    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
                    logDocQueue.drainTo(docs);
                    if (docs.size() > 0) {
                        solrServer.add(docs);
                        solrServer.commit(false, false);
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    /*
     * Removes entries from the in-memory system log index
     *  for an interval defined by config.log.pruning-interval.
    */
    private class LogPruner implements Runnable {
        public void run() {
            while(true) {
                try {
                    String metricPruningInterval = 
                        Mecha.getConfig()
                             .getJSONObject("log")
                             .getString("metric-pruning-interval");

                    String logPruningInterval = 
                        Mecha.getConfig()
                             .getJSONObject("log")
                             .getString("log-pruning-interval");

                    log.info("Pruning metrics on interval " + 
                        metricPruningInterval + ", log on interval " + 
                        logPruningInterval);
                    solrServer.deleteByQuery("bucket:metric AND last_modified:[* TO NOW-" +
                                             metricPruningInterval + "]");
                    solrServer.deleteByQuery("bucket:log AND last_modified:[* TO NOW-" +
                                             logPruningInterval + "]");
                    solrServer.deleteByQuery("bucket:error AND last_modified:[* TO NOW-" +
                                             logPruningInterval + "]");
                    solrServer.commit(false, false);
                    Thread.sleep(60000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    /*
     * Add an entry to the system log index.
    */
    public void error(String name, JSONObject data) {
        log("error", name, null, data);
    }
    
    public void log(String name, String message, JSONObject data) {
        log("log", name, message, data);
    }
    
    public void logMetric(String name, JSONObject data) {
        log("metric", name, null, data);
    }
    
    public void log(String type, String name, String message, JSONObject data) {
        try {
            String id = HashUtils.sha1(Mecha.guid());
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", id);
            doc.addField("bucket", type);
            doc.addField("name", name);
            if (message != null) {
                doc.addField("message", message);
            }
            if (data != null) {
                for(String f: JSONObject.getNames(data)) {
                    if (f.endsWith("_s_mv")) {   // array of exact strings
                        JSONArray mv = data.getJSONArray(f);
                        List<String> vals = new ArrayList<String>();
                        for(int j=0; j<mv.length(); j++) {
                            vals.add(mv.getString(j));
                        }
                        doc.addField(f, vals);
                    } else {
                        doc.addField(f, data.get(f));
                    }
                }
            }
            logDocQueue.put(doc);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    protected void deleteByQuery(String query) throws Exception {
        solrServer.deleteByQuery(query);
    }
}


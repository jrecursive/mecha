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
                    } else {
                        Thread.sleep(100);
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
                    String pruningInterval = 
                        Mecha.getConfig()
                             .getJSONObject("log")
                             .getString("pruning-interval");
                    log.info("Pruning system log on interval " + 
                        pruningInterval);
                    solrServer.deleteByQuery("last_modified:[* TO NOW-" +
                                             pruningInterval + "]");
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
    public void log(String name, JSONObject msg) {
        log("log", name, msg);
    }
    
    public void logMetric(String name, JSONObject msg) {
        log("metric", name, msg);
    }
    
    public void log(String type, String name, JSONObject msg) {
        try {
            String id = HashUtils.sha1(Mecha.guid());
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", id);
            doc.addField("bucket", type);
            doc.addField("name", name);
            for(String f: JSONObject.getNames(msg)) {
                if (f.endsWith("_s_mv")) {   // array of exact strings
                    JSONArray mv = msg.getJSONArray(f);
                    List<String> vals = new ArrayList<String>();
                    for(int j=0; j<mv.length(); j++) {
                        vals.add(mv.getString(j));
                    }
                    doc.addField(f, vals);
                } else {
                    doc.addField(f, msg.get(f));
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


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
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

package mecha.monitoring;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
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
    final private AtomicBoolean stopIndexerThread;
    
    public SystemLog() throws Exception {
        stopIndexerThread = new AtomicBoolean(false);
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
    
    protected void stop() throws Exception {
        stopIndexerThread.set(true);
        prunerThread.interrupt();
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
                        
                        /*
                         * Process documents for log, error, & metric channel taps.
                        */
                        for (SolrInputDocument doc : docs) {
                            try {
                                String channel;
                                String bucket = "" + doc.getFieldValue("bucket");
                                                                        
                                channel = "$" + bucket;
                                
                                if (Mecha.getChannels().channelExists(channel)) {
                                    JSONObject obj = new JSONObject();
                                    for(String f : doc.getFieldNames()) {
                                        obj.put(f, "" + doc.getFieldValue(f));
                                    }
                                    Mecha.getChannels()
                                         .getChannel(channel)
                                         .send(obj);
                                }
                            } catch (java.lang.InterruptedException iex) {
                                log.info("log indexer thread interrupted!");
                                return;
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                    if (stopIndexerThread.get()) {
                        log.info("log indexer thread stopped");
                        return;
                    }
                    Thread.sleep(1000);
                } catch (java.lang.InterruptedException iex) {
                    log.info("log indexer thread interrupted!");
                    return;
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
                    solrServer.deleteByQuery("bucket:metric AND ts:[* TO NOW-" +
                                             metricPruningInterval + "]");
                    solrServer.deleteByQuery("bucket:log AND ts:[* TO NOW-" +
                                             logPruningInterval + "]");
                    solrServer.deleteByQuery("bucket:error AND ts:[* TO NOW-" +
                                             logPruningInterval + "]");
                    Thread.sleep(60000);
                } catch (java.lang.InterruptedException iex) {
                    log.info("pruner thread stopped");
                    return;
                } catch (Exception ex) {
                    try {
                        ex.printStackTrace();
                        Thread.sleep(10000);
                    } catch (Exception ex1) {
                        ex1.printStackTrace();
                    }
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
        } catch (java.lang.InterruptedException iex) {
            log.info("log indexer thread interrupted (shutting down)");
            return;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    protected void deleteByQuery(String query) throws Exception {
        solrServer.deleteByQuery(query);
    }
}


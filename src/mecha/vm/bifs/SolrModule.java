package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;


import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.response.FacetField;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;

public class SolrModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(SolrModule.class.getName());
    
    public SolrModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }
    
    /*
     * "Universal" solr-select (param-based)
     *  Stream implementation.
    */
    public class SelectIterator extends MVMFunction {
        /*
         * Dedicated iterator thread & control channel communication mechanisms.
        */
        final private AtomicBoolean next;
        final private AtomicBoolean stop;
        final private ReentrantLock stateLock;
        final private Thread iteratorThread;
        final private String iterationLabel;
        final private boolean materialize;
    
        public SelectIterator(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            next = new AtomicBoolean(false);
            stop = new AtomicBoolean(false);
            stateLock = new ReentrantLock();
            
            if (config.has("iterator-name")) {
                iterationLabel = config.getString("iterator-name");
            } else {
                iterationLabel = null;
            }
            
            if (config.has("materialize") &&
                config.getString("materialize").equals("true")) {
                materialize = true;
            } else {
                materialize = false;
            }
            
            /*
             * Dedicated bucket iterator thread.
            */
            final Runnable runnableIterator = new Runnable() {
                public void run() {
                    try {
                        JSONObject selectParams = getConfig().getJSONObject("params");
                        long t_st = System.currentTimeMillis();
                        long start = 0;
                        long batchSize = 100;
                        long count = 0;
                        long rowLimit = -1;
                        long rawFound = 0;
                        
                        /*
                         * Rewrite scored queries into filter queries.
                        */
                        if (!selectParams.has("q") &&
                            !selectParams.has("fq")) {
                            selectParams.put("q", "*:*");
                        }
                        if (selectParams.has("q") &&
                            !selectParams.has("fq")) {
                            selectParams.put("fq", selectParams.get("q"));
                            selectParams.put("q", "*:*");
                        }
                        
                        if (selectParams.has("start")) {
                            start = Long.parseLong("" + selectParams.get("start"));
                            selectParams.remove("start");
                        }
                        
                        if (selectParams.has("rows")) {
                            rowLimit = Long.parseLong("" + selectParams.get("rows"));
                            selectParams.remove("rows");
                        }

                        final AtomicBoolean earlyExit = new AtomicBoolean(false);
                        final JSONObject doneMsg = new JSONObject();
                        
                        try {
                            while(true) {
                                if (start > 0 && start < batchSize) break;
                                ModifiableSolrParams solrParams = new ModifiableSolrParams();
                                for(String k : JSONObject.getNames(selectParams)) {
                                    solrParams.set(k, "" + selectParams.get(k));
                                }
                                solrParams.set("start", "" + start);
                                solrParams.set("rows", "" + batchSize);
                                
                                int batchCount = 0;
                                QueryResponse res = 
                                    Mecha.getSolrManager().getIndexServer().query(solrParams);
                                
                                /*
                                 * Document results.
                                */  
                                rawFound = res.getResults().getNumFound();
                                if (res.getResults().getNumFound() == 0) {
                                    log.info("no results! " + getConfig().toString());
                                    break;
                                }
                                if (rowLimit == -1) {
                                    rowLimit = res.getResults().getNumFound();
                                }
                                for(SolrDocument doc : res.getResults()) {
                                    try {
                                        /*
                                         * Wait for iterator control "next" state.
                                        */
                                        
                                        while(!next.get()) {
                                            if (stop.get()) {
                                                // trigger early exit bubble
                                                earlyExit.set(true);
                                                break;
                                            }
                                            Thread.yield();
                                        }
                                        if (earlyExit.get()) {
                                            break;
                                        }
                                        // bubble early exit up
                                        stateLock.lock();
                                        try {
                                            JSONObject msg;
                                            if (materialize) {
                                                msg = materializePBK("" + doc.get("partition"),
                                                                     "" + doc.get("bucket"),
                                                                     "" + doc.get("key"));
                                            } else {
                                                msg = new JSONObject();
                                                for(String fieldName : doc.getFieldNames()) {
                                                    msg.put(fieldName, doc.get(fieldName));
                                                }
                                            }
                                            msg.put("key", "" + doc.get("key"));
                                            msg.put("bucket", "" + doc.get("bucket"));
                                            broadcastDataMessage(msg);
                                            count++;
                                            next.set(false);
                                        } finally {
                                            stateLock.unlock();
                                        }
                                        batchCount++;
                                        if (count >= rowLimit) break;
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                        try {
                                            earlyExit.set(true);
                                            doneMsg.put("iterator-exception",
                                                Mecha.exceptionToStringArray(ex));
                                            break;
                                        } catch (Exception _ex) { _ex.printStackTrace(); break; }
                                    }
                                }
                                start += batchCount;
                                // bubble early exit up
                                if (earlyExit.get()) {
                                    break;
                                }
                                if (start >= rowLimit) break;
                            }
                            long t_elapsed = System.currentTimeMillis() - t_st;
                            
                            doneMsg.put("elapsed", t_elapsed);
                            doneMsg.put("found", rawFound);

                        } catch (Exception ex) {
                            log.info("iterator thread exception!");
                            ex.printStackTrace();
                            try {
                                doneMsg.put("exception", Mecha.exceptionToStringArray(ex));
                            } catch (Exception _ex) { _ex.printStackTrace(); }
                        }
                        doneMsg.put("stopped", earlyExit.get());
                        doneMsg.put("count", count);
                        doneMsg.put("$solr-config", getConfig());
                        log.info("<done> solr iterator " + doneMsg.toString());
                        broadcastDone(doneMsg);
                    } catch (Exception ex1) {
                        ex1.printStackTrace();
                    }
                }
            };
            iteratorThread = new Thread(runnableIterator);
        }
        
        public void onStartEvent(JSONObject startEventMsg) throws Exception {
            iteratorThread.start();
            /*
            while(!iteratorThread.isAlive()) {
                log.info("waiting for iterator thread...");
                Thread.sleep(1000);
            }
            */
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            stop.set(true);
            if (iteratorThread.isAlive()) {
                iteratorThread.interrupt();
            }
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            final String verb = msg.getString("$");
            
            stateLock.lock();
            try {
                /*
                 * "next" - send one record down the pipeline.
                */
                if (verb.equals("next")) {
                    if (!iteratorThread.isAlive()) {
                        return;
                    }
                    next.set(true);

                /*
                 * "stop" - cancel the iteration.
                */
                } else if (verb.equals("stop")) {
                    stop.set(true);
                    
                /*
                 * unknown
                */
                } else {
                    log.info("Unknown state! msg = " + msg.toString(2));
                }
            } finally {
                stateLock.unlock();
            }
        }
    }
    
    /*
     * "Universal" solr-select (param-based)
     *  Stream implementation.
    */
    public class Select extends MVMFunction {
        final private boolean materialize;
        
        public Select(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            if (config.has("materialize") &&
                config.getString("materialize").equals("true")) {
                materialize = true;
            } else {
                materialize = false;
            }
        }
        
        public void onStartEvent(JSONObject startEventMsg) throws Exception {
            JSONObject selectParams = getConfig().getJSONObject("params");
            long t_st = System.currentTimeMillis();
            long start = 0;
            long batchSize = 100;
            long count = 0;
            long rowLimit = -1;
            long rawFound = 0;
            
            /*
             * Rewrite scored queries into filter queries.
            */
            if (!selectParams.has("q") &&
                !selectParams.has("fq")) {
                selectParams.put("q", "*:*");
            }
            if (selectParams.has("q") &&
                !selectParams.has("fq")) {
                selectParams.put("fq", selectParams.get("q"));
                selectParams.put("q", "*:*");
            }
            
            if (selectParams.has("start")) {
                start = Long.parseLong("" + selectParams.get("start"));
                selectParams.remove("start");
            }
            
            if (selectParams.has("rows") && !selectParams.has("facet")) {
                rowLimit = Long.parseLong("" + selectParams.get("rows"));
                selectParams.remove("rows");
            }
            
            if (selectParams.has("facet")) {
                batchSize = 0;
            }
            
            while(true) {
                ModifiableSolrParams solrParams = new ModifiableSolrParams();
                for(String k : JSONObject.getNames(selectParams)) {
                    solrParams.set(k, "" + selectParams.get(k));
                }
                solrParams.set("start", "" + start);
                solrParams.set("rows", "" + batchSize);
                
                int batchCount = 0;
                QueryResponse res = 
                    Mecha.getSolrManager().getIndexServer().query(solrParams);
                
                /*
                 * Facet results.
                */
                if (res.getFacetFields() != null) {
                    for (FacetField facetField : res.getFacetFields()) {
                        if (facetField.getValues() == null) continue;
                        for (FacetField.Count facetFieldCount : facetField.getValues()) {
                            JSONObject msg = new JSONObject();
                            msg.put("field", facetField.getName());
                            msg.put("value", facetFieldCount.getName());
                            msg.put("count", facetFieldCount.getCount());
                            broadcastDataMessage(msg);
                        }
                    }
                    break;
                }
                
                /*
                 * Document results.
                */  
                rawFound = res.getResults().getNumFound();

                /*
                 * "count-only" 'short-circuit'.
                */
                if (getConfig().has("count-only") &&
                    getConfig().getString("count-only").equals("true")) {
                    JSONObject countMsg = new JSONObject();
                    countMsg.put("value", "count");
                    countMsg.put("count", rawFound);
                    broadcastDataMessage(countMsg);
                    broadcastDone();
                    return;
                }
                
                if (res.getResults().getNumFound() == 0) break;
                if (rowLimit == -1) {
                    rowLimit = res.getResults().getNumFound();
                }
                for(SolrDocument doc : res.getResults()) {
                    JSONObject msg;
                    if (materialize) {
                        msg = materializePBK("" + doc.get("partition"),
                                             "" + doc.get("bucket"),
                                             "" + doc.get("key"));
                    } else {
                        msg = new JSONObject();
                        for(String fieldName : doc.getFieldNames()) {
                            msg.put(fieldName, doc.get(fieldName));
                        }
                    }
                    broadcastDataMessage(msg);
                    count++; 
                    batchCount++;
                    if (count >= rowLimit) break;
                }
                //log.info("batchCount: " + batchCount + " start: " + start + " count: " + count + " numFound: " + 
                //    res.getResults().getNumFound());
                start += batchCount;
                if (start >= rowLimit) break;
            }
            long t_elapsed = System.currentTimeMillis() - t_st;
            JSONObject doneMsg = new JSONObject();
            doneMsg.put("elapsed", t_elapsed);
            doneMsg.put("count", count);
            doneMsg.put("found", rawFound);
            broadcastDone(doneMsg);
        }
    }
    
    /*
     * Process stream of faceted value points & reduce on done.
     *  Can be used for anything that passes messages with a
     *  "value" and "count" field (one per pair).
    */
    public class ValueCountReducer extends MVMFunction {
        Map<String, Integer> facetMap;
        
        public ValueCountReducer(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            facetMap = new HashMap<String, Integer>();
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            String term = msg.getString("value");
            int count = msg.getInt("count");
            if (facetMap.containsKey(term)) {
                count += facetMap.get(term);
            }
            facetMap.put(term, count);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            JSONObject dataMsg = new JSONObject();
            for(String term : facetMap.keySet()) {
                dataMsg.put(term, facetMap.get(term));
            }
            broadcastDataMessage(dataMsg);
            broadcastDone(msg);
        }
        
    }
    
    private JSONObject materializePBK(String partition, String bucket, String key) 
        throws Exception {
        JSONObject obj = new JSONObject(
            new String(Mecha.getMDB()
                            .getBucket(partition, bucket)
                            .get(key.getBytes())));
        return new JSONObject(
            obj.getJSONArray("values")
               .getJSONObject(0)
               .getString("data"));
    }

    
}
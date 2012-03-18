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

package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.text.Collator;
import java.text.SimpleDateFormat;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.RangeFacet;

import org.apache.commons.math.stat.StatUtils;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;
import mecha.util.*;
import mecha.monitoring.*;

public class SolrModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(SolrModule.class.getName());
        
    private static String STANDARD_DATE_FORMAT = 
        "yyyy-MM-dd'T'HH:mm:ss";
            
    public SolrModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }
        
    /*
     * Solr-specific optimization of WithSortedCoverage.
     *
     * This functions as WithCoverage does except that
     *  it will equally iterate all upstream functions
     *  via the control message ! (next), systematically
     *  exhausting them.  This behaves as a "composite"
     *  sorting network, using record buffering instead of
     *  a fully expanded network.
     *
     * This function differs from WithSortedCoverage in
     *  that it will rewrite many Solr /select queries on
     *  the same host as one composite query reflecting
     *  all partitions on the destination host as a string
     *  of AND clauses.
     * 
     * This function is deterministic.
     *
    */
    public class CoveredIndexSelect extends MVMFunction {
        final private Map<String, String> refIdToVar;
        final private String hostMarker;
        final private String queryMarker;
        final private Set<String> proxyVars;
        final private String sortField;
        final private boolean isAscending;
        int doneCount = 0;
        int replyCount = 0;
        final private Map<String, JSONObject> refIdToObj;
        final Comparator comparatorFun;
        final Collator collator;
        
        public CoveredIndexSelect(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            proxyVars = new HashSet<String>();
            refIdToVar = new HashMap<String, String>();
            
            /*
             * Order parameters
            */
            sortField = config.getString("sort-field");
            if (config.getString("sort-order").startsWith("asc")) {
                isAscending = true;
            } else {
                isAscending = false;
            }
            
            /*
             * Partition, host replacement strings.
            */
            if (config.has("host-marker")) {
                hostMarker = config.getString("host-marker");
            } else {
                hostMarker = "<<host>>";
            }
            if (config.has("query-marker")) {
                queryMarker = config.getString("query-marker");
            } else {
                queryMarker = "<<query>>";
            }
            refIdToObj = new HashMap<String, JSONObject>();
            
            /*
             * Sort mechanism.
            */
            collator = Collator.getInstance();
            comparatorFun = new Comparator<JSONObject>() {
                public int compare(JSONObject obj1, JSONObject obj2) {
                    try {
                        String value1 = "" + obj1.get(sortField);
                        String value2 = "" + obj2.get(sortField);
                        if (isAscending) {
                            return collator.compare(value1, value2);
                        } else {
                            return collator.compare(value2, value1);
                        }
                    } catch (Exception ex) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", ex);
                        ex.printStackTrace();
                    }
                    return 0;
                }
            };
        }
        
        public void onPostAssignment(MVMContext ctx, String thisInstVar, JSONObject config) throws Exception {
            final String bucket = config.getString("bucket");
            final Map<String, Set<String>> coveragePlan = 
                Mecha.getRiakRPC().getCoverage(bucket);
            
            for(String host : coveragePlan.keySet()) {
                
                /*
                 * Compose composite query accounting for all
                 *  partitions located on host.
                 *
                 * e.g., (partition:0 OR partition:1234 OR ...) AND
                 *       bucket:some_bucket ...
                 *
                */
                final StringBuffer partitionQuery = new StringBuffer();
                for(String partition : coveragePlan.get(host)) {
                    partitionQuery.append("partition:");
                    partitionQuery.append(partition);
                    partitionQuery.append(" OR ");
                }
                partitionQuery.append("\n");
                String query = partitionQuery.toString().replace(" OR \n", "");
                
                log.info("query = " + query);
                
                String doVerb = config.getJSONObject("do").getString("$");
                String proxyVar = 
                    HashUtils.sha1(Mecha.guid(CoveredIndexSelect.class)) + 
                        "-" + host + 
                        "-" + doVerb;
                proxyVars.add(proxyVar);
                
                String doAstStr = config.getJSONObject("do").toString();
                doAstStr = doAstStr.replaceAll(hostMarker, host);
                doAstStr = doAstStr.replaceAll(queryMarker, query);
                JSONObject doAst = new JSONObject(doAstStr);
                String vertexRefId = 
                    Mecha.getMVM().nativeAssignment(getContext(), proxyVar, doAst);
                Mecha.getMVM().nativeFlowAddEdge(getContext(), proxyVar, thisInstVar);
                refIdToVar.put(vertexRefId, proxyVar);
            }
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
                JSONObject nextMsg = new JSONObject();
                nextMsg.put("$", "next");
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, nextMsg);
            }
        }
        
        /*
         * Forward all control messages upstream.
        */
        public void onControlMessage(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
            }
        }
        
        /*
         * Forward all cancel messages upstream.
        */
        public void onCancelEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
            }
        }
        
        /*
         * Forward all data messages downstream.
        */
        public void onDataMessage(JSONObject msg) throws Exception {
            String origin = msg.getString("$origin");
            if (refIdToObj.containsKey(origin)) {
                log.info("\n\n\n!! This should never happen! " + origin + " \n\n\n\n");
            }
            refIdToObj.put(origin, msg);
            processTable();
        }
        
        private void processTable() throws Exception {
            if (refIdToObj.keySet().size() == (proxyVars.size() - doneCount)) {
                JSONObject[] values = 
                    (JSONObject[]) refIdToObj.values().toArray(new JSONObject[0]);
                if (values.length == 0) return;
                Arrays.<JSONObject>sort(values, comparatorFun);
                JSONObject winner = values[0];
                String winnerOrigin = 
                    winner.getString("$origin");
                
                // remove "winning origin" entry from map
                refIdToObj.remove(winnerOrigin);
                
                /*
                 * Send a "next" control message to the
                 *  origin function of the "winner".
                */
                String proxyVar = refIdToVar.get(winnerOrigin);
                JSONObject nextMsg = new JSONObject();
                nextMsg.put("$", "next");
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, nextMsg);
                
                // broadcast "winning origin" value
                broadcastDataMessage(winner);
            }
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            doneCount++;
            if (doneCount == proxyVars.size()) {
            
                /*
                 * Empty the remaining entries in the map.
                */
                JSONObject[] values = 
                    (JSONObject[]) refIdToObj.values().toArray(new JSONObject[0]);
                Arrays.<JSONObject>sort(values, comparatorFun);
                for(JSONObject obj: values) {
                    broadcastDataMessage(obj);
                }
                
                JSONObject doneMsg = new JSONObject();
                doneMsg.put("complete", doneCount);
                broadcastDone(doneMsg);
            } else {
                processTable();
            }
        }
    }
    
    /*
     *
     * "Universal" solr-select (param-based)
     *
     * Iterator implementation.
     *
    */
    public class SelectIterator extends MVMFunction {
        /*
         * Dedicated iterator thread & control channel communication mechanisms.
        */
        final private Semaphore next;
        final private AtomicBoolean stop;
        final private ReentrantLock stateLock;
        final private Thread iteratorThread;
        final private String iterationLabel;
        final private boolean materialize;
        final private String core;
    
        public SelectIterator(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            next = new Semaphore(1, true);
            stop = new AtomicBoolean(false);
            stateLock = new ReentrantLock();
            next.acquire();
            
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
            
            if (config.has("core")) {
                core = config.<String>get("core");
            } else {
                core = "index";
            }
            
            /*
             * Dedicated bucket iterator thread.
            */
            final Runnable runnableIterator = new Runnable() {
                final private SimpleDateFormat dateFormat = 
                    new java.text.SimpleDateFormat(STANDARD_DATE_FORMAT);

                public void run() {
                    try {
                        JSONObject selectParams = getConfig().getJSONObject("params");
                        long t_st = System.currentTimeMillis();
                        long start = 0;
                        long batchSize = 500;
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
                        
                        log.info("fq = " + selectParams.get("fq"));
                        
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
                                    String paramVal = "" + selectParams.get(k);
                                    if (k.equals("fl")) {
                                        if (paramVal.indexOf("bucket") == -1) {
                                            paramVal += ",bucket";
                                        }
                                        if (paramVal.indexOf("key") == -1) {
                                            paramVal += ",key";
                                        }
                                        if (paramVal.indexOf("partition") == -1) {
                                            paramVal += ",partition";
                                        }
                                    }
                                    solrParams.set(k, paramVal);
                                }
                                solrParams.set("start", "" + start);
                                solrParams.set("rows", "" + batchSize);
                                
                                int batchCount = 0;
                                long solr_t_st = System.currentTimeMillis();
                                QueryResponse res = 
                                    Mecha.getSolrManager().getSolrServer(core).query(solrParams);
                                
                                Mecha.getMonitoring()
                                     .metric("mecha.vm.bifs.solr-module." + 
                                                core + ".select-iterator.query.ms",
                                             System.currentTimeMillis() - solr_t_st);
                                
                                rawFound = res.getResults().getNumFound();
                                if (start == rawFound) break;
                                if (res.getResults().getNumFound() == 0) {
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
                                        next.acquire();
                                        if (stop.get()) {
                                            // trigger early exit bubble
                                            earlyExit.set(true);
                                            break;
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
                                                    if (fieldName.equals("last_modified") ||
                                                        fieldName.endsWith("_dt")) {
                                                        String date = 
                                                            dateFormat.format((Date)doc.get(fieldName));
                                                        msg.put(fieldName, date);
                                                    } else {
                                                        msg.put(fieldName, doc.get(fieldName));
                                                    }
                                                }
                                            }
                                            msg.put("key", "" + doc.get("key"));
                                            msg.put("bucket", "" + doc.get("bucket"));
                                            broadcastDataMessage(msg);
                                            count++;
                                        } finally {
                                            stateLock.unlock();
                                        }
                                        batchCount++;
                                        if (count >= rowLimit) break;
                                    } catch (java.lang.InterruptedException iex) {
                                        return;

                                    } catch (Exception ex) {
                                        Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", ex);
                                        ex.printStackTrace();
                                        try {
                                            earlyExit.set(true);
                                            doneMsg.put("iterator-exception",
                                                Mecha.exceptionToStringArray(ex));
                                            break;
                                        } catch (Exception _ex) {
                                            Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", _ex);
                                            _ex.printStackTrace(); break;
                                        }
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
                        } catch (java.lang.InterruptedException iex) {
                            return;
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", ex);
                            log.info("iterator thread exception!");
                            ex.printStackTrace();
                            try {
                                doneMsg.put("exception", Mecha.exceptionToStringArray(ex));
                            } catch (Exception _ex) {
                                Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", _ex);
                                _ex.printStackTrace();
                            }
                        }
                        doneMsg.put("stopped", earlyExit.get());
                        doneMsg.put("count", count);
                        doneMsg.put("$solr-config", getConfig());
                        broadcastDone(doneMsg);
                    } catch (Exception ex1) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", ex1);
                        ex1.printStackTrace();
                    }
                }
            };
            iteratorThread = new Thread(runnableIterator);
        }
        
        public void onStartEvent(JSONObject startEventMsg) throws Exception {
            iteratorThread.start();
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
                    next.release();

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
     *
     *  Stream implementation.
     *
    */
    public class Select extends MVMFunction {
        final private boolean materialize;
        final private boolean deleteByQuery;
        final private String core;
        final private SimpleDateFormat dateFormat = 
            new java.text.SimpleDateFormat(STANDARD_DATE_FORMAT);
        final private Thread iteratorThread;
        
        public Select(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            if (config.has("materialize") &&
                config.getString("materialize").equals("true")) {
                materialize = true;
            } else {
                materialize = false;
            }
            
            if (config.has("core")) {
                core = config.<String>get("core");
            } else {
                core = "index";
            }
            
            if (config.has("delete-by-query") &&
                config.<String>get("delete-by-query").equals("true")) {
                deleteByQuery = true;
            } else {
                deleteByQuery = false;
            }
            
            iteratorThread = new Thread(new Runnable() {
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
                                // to be able to materialize, etc. always include
                                //  the bucket,key field
                                String paramVal = "" + selectParams.get(k);
                                if (k.equals("fl")) {
                                    if (paramVal.indexOf("bucket") == -1) {
                                        paramVal += ",bucket";
                                    }
                                    if (paramVal.indexOf("key") == -1) {
                                        paramVal += ",key";
                                    }
                                    if (paramVal.indexOf("partition") == -1) {
                                        paramVal += ",partition";
                                    }
                                }
                                solrParams.set(k, paramVal);
                            }
                            solrParams.set("start", "" + start);
                            solrParams.set("rows", "" + batchSize);
                            
                            int batchCount = 0;
                            long solr_t_st = System.currentTimeMillis();
                            QueryResponse res = 
                                Mecha.getSolrManager().getSolrServer(core).query(solrParams);
                            Mecha.getMonitoring().metric("mecha.vm.bifs.solr-module." + core + ".select.query.ms",
                                                         System.currentTimeMillis() - solr_t_st);
                            
                            /*
                             * Range-facet results.
                            */
                            
                            if (res.getFacetRanges() != null) {
                                for (RangeFacet rangeFacet : res.getFacetRanges()) {
                                    if (rangeFacet.getCounts() == null) continue;
                                    String fieldName = 
                                        rangeFacet.getStart() + " " +
                                        rangeFacet.getEnd() + " " +
                                        rangeFacet.getName();
                                    for(RangeFacet.Count rangeFacetCount: (List<RangeFacet.Count>) rangeFacet.getCounts()) {
                                        JSONObject msg = new JSONObject();
                                        msg.put("field", fieldName);
                                        msg.put("value", rangeFacetCount.getValue());
                                        msg.put("count", rangeFacetCount.getCount());
                                        broadcastDataMessage(msg);
                                    }
                                }
                            }
                            
                            /*
                             * Facet results.
                            */
                            if (res.getFacetFields() != null) {
                                for (FacetField facetField : res.getFacetFields()) {
                                    if (getConfig().has("cardinality-only") &&
                                        getConfig().<String>get("cardinality-only").equals("true")) {
                                        JSONObject dataMsg = new JSONObject();
                                        dataMsg.put("value", 
                                            Mecha.getHost() + "-" + 
                                            getConfig().<String>get("partition") + "-" + 
                                            "cardinality");
                                        dataMsg.put("count", facetField.getValueCount());
                                        broadcastDataMessage(dataMsg);
                                        broadcastDone();
                                        return;
                                    }
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
                            
                            if (start == rawFound) break;
                            if (res.getResults().getNumFound() == 0) break;
                            if (rowLimit == -1) {
                                rowLimit = res.getResults().getNumFound();
                            }
                            for(SolrDocument doc : res.getResults()) {
                                /*
                                 * delete query
                                */
                                if (deleteByQuery) {
                                    String objPartition = "" + doc.get("partition");
                                    String objBucket = "" + doc.get("bucket");
                                    String objKey = "" + doc.get("key");
                                    if (core.equals("index")) {
                                        Mecha.getMDB()
                                             .getBucket(objPartition, objBucket)
                                             .delete(objKey.getBytes());
                                    } else {
                                        Mecha.getSolrManager()
                                             .getSolrServer(core)
                                             .deleteByQuery("bucket:" + objBucket + " AND " +
                                                            "partition:" + objPartition + " AND " +
                                                            "key:\"" + objKey + "\"");
                                    }
                                /* 
                                 * non-delete query
                                */
                                } else {
                                    JSONObject msg;
                                    if (materialize) {
                                        msg = materializePBK("" + doc.get("partition"),
                                                             "" + doc.get("bucket"),
                                                             "" + doc.get("key"));
                                    } else {
                                        msg = new JSONObject();
                                        for(String fieldName : doc.getFieldNames()) {
                                            if (fieldName.equals("last_modified") ||
                                                fieldName.endsWith("_dt")) {
                                                String date = 
                                                    dateFormat.format((Date)doc.get(fieldName));
                                                msg.put(fieldName, date);
                                            } else {
                                                msg.put(fieldName, doc.get(fieldName));
                                            }
                                        }
                                    }
                                    msg.put("key", "" + doc.get("key"));
                                    msg.put("bucket", "" + doc.get("bucket"));
                                    broadcastDataMessage(msg);
                                }
                                
                                count++; 
                                batchCount++;
                                if (count >= rowLimit) break;
                            }
                            start += batchCount;
                            if (start >= rowLimit) break;
                        }
                        long t_elapsed = System.currentTimeMillis() - t_st;
                        
                        JSONObject doneMsg = new JSONObject();
                        doneMsg.put("elapsed", t_elapsed);
                        doneMsg.put("count", count);
                        if (deleteByQuery) {
                            Mecha.getSolrManager()
                                 .getSolrServer(core)
                                 .commit(true, true);
                            doneMsg.put("deleted", rawFound);
                        } else {
                            doneMsg.put("found", rawFound);
                        }
                        broadcastDone(doneMsg);
                        
                    } catch (java.lang.InterruptedException iex) {
                        return;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        Mecha.getMonitoring().error("mecha.vm.bifs.solr-module.select", ex);
                        broadcastDone();
                        return;
                    }
                }
            });
        }
        
        public void onStartEvent(JSONObject startEventMsg) throws Exception {
            iteratorThread.start();
        }
        
        public void onCancel(JSONObject msg) throws Exception {
            iteratorThread.interrupt();
        }
    }
    
    /*
     * sort an arbitrary list of accumulated objects by
     *  specified field value
    */
    public class SortedAccumulatingReducer extends MVMFunction {
        final private List<JSONObject> objs;
        final private String sortField;
        final private boolean isAscending;
        final Comparator comparatorFun;
        final Collator collator;

        public SortedAccumulatingReducer(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            objs = new ArrayList<JSONObject>();
            sortField = config.<String>get("sort-field");
            
            // default to ascending sort
            if (config.has("sort-dir")) {
                if (config.<String>get("sort-dir").startsWith("asc")) {
                    isAscending = true;
                } else {
                    isAscending = false;
                }
            } else {
                isAscending = true;
            }
            
            collator = Collator.getInstance();
            comparatorFun = new Comparator<JSONObject>() {
                public int compare(JSONObject obj1, JSONObject obj2) {
                    try {
                        String value1 = "" + obj1.get(sortField);
                        String value2 = "" + obj2.get(sortField);
                        if (isAscending) {
                            return collator.compare(value1, value2);
                        } else {
                            return collator.compare(value2, value1);
                        }
                    } catch (Exception ex) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.solr-module", ex);
                        ex.printStackTrace();
                    }
                    return 0;
                }
            };
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            objs.add(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            JSONObject[] values = objs.toArray(new JSONObject[0]);
            if (values.length > 0) {
                Arrays.<JSONObject>sort(values, comparatorFun);
                JSONObject dataMsg = new JSONObject();
                JSONArray jsonArray = new JSONArray();
                for(JSONObject obj : values) {
                    jsonArray.put(obj);
                }
                dataMsg.put("data", jsonArray);
                broadcastDataMessage(dataMsg);
            }
            broadcastDone(msg);
        }
    }
    
    /*
     * Produce an average of the values of all keys
     *  to 'estimate' the cardinality values to a useful 
     *  degree.
    */
    public class CardinalityReducer extends MVMFunction {
        final private Set<Integer> values;
        final private JSONObject valueMap;
        
        public CardinalityReducer(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            values = new HashSet<Integer>();
            valueMap = new JSONObject();
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            String term = msg.getString("value");
            int count = msg.getInt("count");
            valueMap.put(term, count);
            
            /*
             * Here we ignore key because it's largely irrelevant.
            */
            values.add(count);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            int pos = 0;
            double[] dvals = new double[values.size()];
            for(int value : values) {
                dvals[pos] = (double) value;
                pos++;
            }
            JSONObject result = new JSONObject();
            result.put("by-partition", valueMap);
            result.put("average", StatUtils.mean(dvals));
            result.put("max", StatUtils.max(dvals));
            result.put("min", StatUtils.min(dvals));
            
            broadcastDataMessage(result);
            broadcastDone(msg);
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
    
    /*
     * Process stream of faceted value points & reduce on done.
     *  Can be used for anything that passes messages with a
     *  "field", "value" and "count" object. each distinct
     *  field value keeps it's own value/count tallies.
    */
    public class FieldValueCountReducer extends MVMFunction {
        Map<String, Map<String, Integer>> fieldFacetMap;
        
        public FieldValueCountReducer(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            fieldFacetMap = new HashMap<String, Map<String, Integer>>();
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            Map<String, Integer> facetMap;
            String field = msg.getString("field");
            if (fieldFacetMap.containsKey(field)) {
                facetMap = fieldFacetMap.get(field);
            } else {
                facetMap = new HashMap<String, Integer>();
                fieldFacetMap.put(field, facetMap);
            }
            String term = msg.getString("value");
            int count = msg.getInt("count");
            if (facetMap.containsKey(term)) {
                count += facetMap.get(term);
            }
            facetMap.put(term, count);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            JSONObject dataMsg = new JSONObject();
            for(String field : fieldFacetMap.keySet()) {
                JSONObject facetObj = new JSONObject();
                Map<String, Integer> facetMap = fieldFacetMap.get(field);
                for(String term : facetMap.keySet()) {
                    facetObj.put(term, facetMap.get(term));
                }
                dataMsg.put(field, facetObj);
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
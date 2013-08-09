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

package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.lang.ref.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.text.Collator;

import mecha.Mecha;
import mecha.jinterface.*;
import mecha.util.HashUtils;
import mecha.json.*;
import mecha.vm.*;
import mecha.client.*;
import mecha.client.net.*;

public class ClusterModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ClusterModule.class.getName());
    
    public ClusterModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }
    
    /*
     * Warp allows a 'transparent proxy' or 'delegate connection'
     *  to another mecha node.  Anything executed remotely is passed
     *  downstream to any local consumers; any control message sent
     *  to the local Warp instance is passed to the remote node.
    */    
    public class Warp extends MVMFunction {
        class WarpDelegate extends MechaClientHandler {
            final String host;
            final TextClient textClient;
            final Semaphore ready;
            final String localFunRefId;
            
            public WarpDelegate(String host, Warp fun) throws Exception {
                this.host = host;
                final String password = Mecha.getConfig().getString("password");
                final int port = Mecha.getConfig().getInt("server-port");
                ready = new Semaphore(1,true);
                ready.acquire();
                localFunRefId = fun.getDataChannelName();
                textClient = new TextClient(host, port, password, this);
            }
            
            public void waitUntilReady() throws Exception {
                ready.acquire();
            }
            
            public void close() throws Exception {
                textClient.close();
            }
            
            public void onMessage(String message) {
                if (message.equals("OK") ||
                    message.startsWith(":OK")) {
                } else {
                    try {
                        JSONObject msg = new JSONObject(message);
                        
                        // Channel message
                        if (msg.has("c") && msg.has("o")) {
                            final String channel = msg.getString("c");
                            final JSONObject obj = msg.getJSONObject("o");
                            obj.put("$delegate-channel", channel);
                            if (obj.has("$") &&
                                obj.getString("$").equals("done")) {
                                sendData(localFunRefId, obj);
                            } else if (obj.has("$") &&
                                obj.getString("$").equals("control")) {
                                sendControl(localFunRefId, obj);
                            } else if (obj.has("$") &&
                                obj.getString("$").equals("cancel")) {
                                sendControl(localFunRefId, obj);
                            } else {
                                sendData(localFunRefId, obj);
                            }
                            
                        // unknown json message?
                        } else {
                            log.info("WarpDelegate: <" + host + "/system-message> " + 
                                msg.toString(2));
                        }
                        
                    // unknown non-json message?
                    } catch (Exception ex) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.cluster-module", ex);
                        ex.printStackTrace();
                        log.info("WarpDelegate: <" + host + "/info> " + message);
                    }
                }
            }
            
            public void onOpen() {
                ready.release();
            }
            
            public void onClose() {
                try {
                    textClient.close();
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.vm.bifs.cluster-module", ex);
                    ex.printStackTrace();
                }
            }
                    
            public void onError() {
                try {
                    textClient.close();
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.vm.bifs.cluster-module", ex);
                    ex.printStackTrace();
                }
            }
            
            public void send(String cmd) throws Exception {
                textClient.send(cmd);
            }

        }
        
        private final String host;
        private final String remoteVar;
        private final JSONObject doAst;
        private final WarpDelegate warpDelegate;
        private final long timeout;
        
        public Warp(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            host = config.getString("host");
            remoteVar = "warp-var";
            if (config.get("do") instanceof String) {
                doAst = new JSONObject();
                doAst.put("$", config.<String>get("do"));
            } else {
                doAst = config.getJSONObject("do");
            }
            
            if (config.has("timeout")) {
                timeout = Long.parseLong(config.getString("timeout"));
            } else {
                timeout = 60000;
            }
            warpDelegate = new WarpDelegate(host, this);
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            // FROM remote
            if (msg.has("$delegate-channel")) {
                log.info("Remote startEvent <" + host + "> " + msg.toString(2));
                
            // TO remote
            } else {
                warpDelegate.waitUntilReady();
                for(Map.Entry<String,List<String>> entry : getContext().getBlockMap().entrySet()) {
                    String blockName = entry.getKey();
                    List<String> blockCode = entry.getValue();
                    warpDelegate.send("#define " + blockName);
                    for(String line : blockCode) {
                        warpDelegate.send(line);
                    }
                    warpDelegate.send("#end " + blockName);
                }
                warpDelegate.send("$assign " + remoteVar + " " + doAst.toString());
                warpDelegate.send("me = (client-sink)");
                warpDelegate.send(remoteVar + " -> me");
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
            }
        }
        
        /*
         * Forward all control messages upstream.
        */
        public void onControlMessage(JSONObject msg) throws Exception {
            // FROM remote
            if (msg.has("$delegate-channel")) {
                broadcastControlMessage(msg);
                
            // TO remote
            } else {
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
            }
        }
        
        /*
         * Forward all cancel messages upstream.
        */
        public void onCancelEvent(JSONObject msg) throws Exception {
            // FROM remote
            if (msg.has("$delegate-channel")) {
                broadcastControlMessage(msg);
                warpDelegate.close();
                
            // TO remote
            } else {
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
                warpDelegate.close();
            }
        }
        
        /*
         * Forward all data messages downstream.
        */
        public void onDataMessage(JSONObject msg) throws Exception {
            // FROM remote
            if (msg.has("$delegate-channel")) {
                broadcastDataMessage(msg);
            
            // TO remote
            } else {
                warpDelegate.send("$data " + remoteVar + " " + msg.toString());
            }
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                broadcastDone(msg);
                warpDelegate.close();
                
            } else {
                // TO remote
                warpDelegate.send("$data " + remoteVar + " " + msg.toString());
            }
        }
    }
    
    /*
     * WithClusterCoverage executes an embedded command (via "do:(...)")
     *  with automatic replacement of <<host>> on all mecha nodes.
     *
     * NOTE: Inputs are immediately passed through in the order they 
     *  are received -- this means this function is non-deterministic!
     *
    */
    public class WithClusterCoverage extends MVMFunction {
        final private String hostMarker;
        final private Set<String> proxyVars;
        int doneCount = 0;
        
        public WithClusterCoverage(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            proxyVars = new HashSet<String>();
            if (config.has("host-marker")) {
                hostMarker = config.getString("host-marker");
            } else {
                hostMarker = "<<host>>";
            }
        }
        
        public void onPostAssignment(MVMContext ctx, String thisInstVar, JSONObject config) throws Exception {
            final String bucket = config.getString("bucket");
            final Set<String> clusterHosts = Mecha.getRiakRPC().getClusterHosts();
            
            for(String host : clusterHosts) {
                String proxyVar = HashUtils.sha1(Mecha.guid(WithClusterCoverage.class)) + "-" + host;
                proxyVars.add(proxyVar);
                
                String doAstStr = config.getJSONObject("do").toString();
                doAstStr = doAstStr.replaceAll(hostMarker, host);
                JSONObject doAst = new JSONObject(doAstStr);
                Mecha.getMVM().nativeAssignment(getContext(), proxyVar, doAst);
                Mecha.getMVM().nativeFlowAddEdge(getContext(), proxyVar, thisInstVar);
            }
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
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
            broadcastDataMessage(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            doneCount++;
            if (doneCount == proxyVars.size()) {
                JSONObject doneMsg = new JSONObject();
                doneMsg.put("complete", doneCount);
                broadcastDone(doneMsg);
            }
        }
    }
    
    /*
     * WithCoverage executes an embedded command (via "do:(...)")
     *  with automatic replacement of <<partition>>, <<host>>.
     *
     * NOTE: Inputs are immediately passed through in the order they 
     *  are received -- this means this function is non-deterministic!
     * 
     * See WithIteratedCoverage for a near-equivalent that is deterministic.
     *
    */
    public class WithCoverage extends MVMFunction {
        
        final private String partitionMarker;
        final private String hostMarker;
        final private Set<String> proxyVars;
        int doneCount = 0;
        
        public WithCoverage(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            proxyVars = new HashSet<String>();
            if (config.has("partition-marker")) {
                partitionMarker = config.getString("partition-marker");
            } else {
                partitionMarker = "<<partition>>";
            }
            if (config.has("host-marker")) {
                hostMarker = config.getString("host-marker");
            } else {
                hostMarker = "<<host>>";
            }
        }
        
        public void onPostAssignment(MVMContext ctx, String thisInstVar, JSONObject config) throws Exception {
            final String bucket = config.getString("bucket");
            final Map<String, Set<String>> coveragePlan = 
                Mecha.getRiakRPC().getCoverage(bucket);
            
            for(String host : coveragePlan.keySet()) {
                for(String partition : coveragePlan.get(host)) {
                    String proxyVar = HashUtils.sha1(Mecha.guid(WithCoverage.class)) + "-" + host;
                    proxyVars.add(proxyVar);
                    
                    String doAstStr = config.getJSONObject("do").toString();
                    doAstStr = doAstStr.replaceAll(hostMarker, host);
                    doAstStr = doAstStr.replaceAll(partitionMarker, partition);
                    JSONObject doAst = new JSONObject(doAstStr);
                    Mecha.getMVM().nativeAssignment(getContext(), proxyVar, doAst);
                    Mecha.getMVM().nativeFlowAddEdge(getContext(), proxyVar, thisInstVar);
                }
            }
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
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
                try {
                    Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
                
                /*
                 * Already destroyed, ignore.
                */
                } catch (java.lang.NullPointerException ex) {
                    // do not log this message.
                    log.info("<cancel> proxyVar:" + proxyVar + " already dead.");
                }
            }
        }
        
        /*
         * Forward all data messages downstream.
        */
        public void onDataMessage(JSONObject msg) throws Exception {
            broadcastDataMessage(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            doneCount++;
            if (doneCount == proxyVars.size()) {
                JSONObject doneMsg = new JSONObject();
                doneMsg.put("complete", doneCount);
                broadcastDone(doneMsg);
            }
        }
    }
    
    /*
     * WithSortedCoverage
     *
     * This functions as WithCoverage does except that
     *  it will equally iterate all upstream functiosn
     *  via the control message ! (next), systematically
     *  exhausting them.  This behaves as a "composite"
     *  sorting network, using record buffering instead of
     *  a fully expanded network.
     * 
     * This function is deterministic.
     *
    */
    public class WithSortedCoverage extends MVMFunction {
        final private Map<String, String> refIdToVar;
        final private String partitionMarker;
        final private String hostMarker;
        final private Set<String> proxyVars;
        final private String sortField;
        final private boolean isAscending;
        int doneCount = 0;
        int replyCount = 0;
        final private Map<String, JSONObject> refIdToObj;
        final Comparator comparatorFun;
        final Collator collator;
        
        public WithSortedCoverage(String refId, MVMContext ctx, JSONObject config) throws Exception {
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
             * Partition, host eplacement strings.
            */
            if (config.has("partition-marker")) {
                partitionMarker = config.getString("partition-marker");
            } else {
                partitionMarker = "<<partition>>";
            }
            if (config.has("host-marker")) {
                hostMarker = config.getString("host-marker");
            } else {
                hostMarker = "<<host>>";
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
                        Mecha.getMonitoring().error("mecha.vm.bifs.cluster-module", ex);
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
                for(String partition : coveragePlan.get(host)) {
                    String doVerb = config.getJSONObject("do").getString("$");
                    String proxyVar = 
                        HashUtils.sha1(Mecha.guid(WithSortedCoverage.class)) + 
                            "-" + host + 
                            "-" + doVerb;
                    proxyVars.add(proxyVar);
                    
                    String doAstStr = config.getJSONObject("do").toString();
                    doAstStr = doAstStr.replaceAll(hostMarker, host);
                    doAstStr = doAstStr.replaceAll(partitionMarker, partition);
                    JSONObject doAst = new JSONObject(doAstStr);
                    String vertexRefId = 
                        Mecha.getMVM().nativeAssignment(getContext(), proxyVar, doAst);
                    Mecha.getMVM().nativeFlowAddEdge(getContext(), proxyVar, thisInstVar);
                    refIdToVar.put(vertexRefId, proxyVar);
                }
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
     * WithIteratedCoverage
     *
     * This functions as WithCoverage does except that
     *  it will equally iterate all upstream functions
     *  via the control message ! (next), systematically
     *  exhausting them.
     * 
     * This function is deterministic.
     *
    */
    public class WithIteratedCoverage extends MVMFunction {
        final private String partitionMarker;
        final private String hostMarker;
        final private Set<String> proxyVars;
        int doneCount = 0;
        int replyCount = 0;
        JSONArray batch;
        
        public WithIteratedCoverage(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            proxyVars = new HashSet<String>();
            if (config.has("partition-marker")) {
                partitionMarker = config.getString("partition-marker");
            } else {
                partitionMarker = "<<partition>>";
            }
            if (config.has("host-marker")) {
                hostMarker = config.getString("host-marker");
            } else {
                hostMarker = "<<host>>";
            }
            batch = new JSONArray();
        }
        
        public void onPostAssignment(MVMContext ctx, String thisInstVar, JSONObject config) throws Exception {
            final String bucket = config.getString("bucket");
            final Map<String, Set<String>> coveragePlan = 
                Mecha.getRiakRPC().getCoverage(bucket);
            
            for(String host : coveragePlan.keySet()) {
                for(String partition : coveragePlan.get(host)) {
                    String proxyVar = HashUtils.sha1(Mecha.guid(WithCoverage.class)) + "-" + host;
                    proxyVars.add(proxyVar);
                    
                    String doAstStr = config.getJSONObject("do").toString();
                    doAstStr = doAstStr.replaceAll(hostMarker, host);
                    doAstStr = doAstStr.replaceAll(partitionMarker, partition);
                    JSONObject doAst = new JSONObject(doAstStr);
                    Mecha.getMVM().nativeAssignment(getContext(), proxyVar, doAst);
                    Mecha.getMVM().nativeFlowAddEdge(getContext(), proxyVar, thisInstVar);
                }
            }
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
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
            replyCount++;
            batch.put(msg);
            if (replyCount == (proxyVars.size() - doneCount)) {
                replyCount = 0;
                JSONObject batchMsg = new JSONObject();
                batchMsg.put("data", batch);
                broadcastDataMessage(batchMsg);
                batch = new JSONArray();
            }
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            doneCount++;
            if (doneCount == proxyVars.size()) {
                JSONObject doneMsg = new JSONObject();
                doneMsg.put("complete", doneCount);
                broadcastDone(doneMsg);
            }
        }
    }

}
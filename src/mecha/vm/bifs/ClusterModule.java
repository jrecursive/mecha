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
                final int port = Mecha.getConfig().getInt("client-port");
                ready = new Semaphore(1,true);
                localFunRefId = fun.getDataChannelName();
                textClient = new TextClient(host, port, password, this);
                ready.release();
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
                            if (obj.has("$type") &&
                                obj.getString("$type").equals("done")) {
                                sendControl(localFunRefId, obj);
                            } else if (obj.has("$type") &&
                                obj.getString("$type").equals("control")) {
                                sendControl(localFunRefId, obj);
                            } else if (obj.has("$type") &&
                                obj.getString("$type").equals("cancel")) {
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
                    ex.printStackTrace();
                }
            }
                    
            public void onError() {
                try {
                    textClient.close();
                } catch (Exception ex) {
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
            doAst = config.getJSONObject("do");
            
            if (config.has("timeout")) {
                timeout = Long.parseLong(config.getString("timeout"));
            } else {
                timeout = 60000;
            }
            
            warpDelegate = new WarpDelegate(host, this);
            //log.info("Warp delegate connecting to <" + host + ">");
        }
        
        /*
         * Forward all start events upstream.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                log.info("Remote startEvent <" + host + "> " + msg.toString(2));
            } else {
                // TO remote
                //log.info("waiting until warp delegate is ready... ");
                //warpDelegate.waitUntilReady(timeout);
                //log.info("Warp delegate successfully connected to <" + host + ">");
                
                //log.info("Transferring " + getContext().getBlockMap().keySet().size() + " blocks..");
                for(Map.Entry<String,List<String>> entry : getContext().getBlockMap().entrySet()) {
                    String blockName = entry.getKey();
                    List<String> blockCode = entry.getValue();
                    warpDelegate.send("#define " + blockName);
                    for(String line : blockCode) {
                        warpDelegate.send(line);
                    }
                    warpDelegate.send("#end " + blockName);
                    //log.info("blocks: " + blockName + ": ok");
                }
                
                //log.info("Registering '" + remoteVar + "' on <" + host + ">");
                warpDelegate.send("$assign " + remoteVar + " " + doAst.toString());
                warpDelegate.send("me = (client-sink)");
                warpDelegate.send(remoteVar + " -> me");
                //log.info("Registration complete on <"  + host + ">");
                //log.info("delegate ready!  starting...");
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
            }
        }
        
        /*
         * Forward all control messages upstream.
        */
        public void onControlMessage(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                //log.info("Remote controlMessage <" + host + "> " + msg.toString(2));
                broadcastControlMessage(msg);
            } else {
                // TO remote
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
            }
        }
        
        /*
         * Forward all cancel messages upstream.
        */
        public void onCancelEvent(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                //log.info("Remote cancelEvent <" + host + "> " + msg.toString(2));
                broadcastControlMessage(msg);
            } else {
                // TO remote
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
                warpDelegate.close();
            }
        }
        
        /*
         * Forward all data messages downstream.
        */
        public void onDataMessage(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                broadcastDataMessage(msg);
            } else {
                // TO remote
                warpDelegate.send("$data " + remoteVar + " " + msg.toString());
            }
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            if (msg.has("$delegate-channel")) {
                // FROM remote
                //log.info("Remote doneEvent <" + host + "> " + msg.toString(2));
                broadcastDone(msg);
            } else {
                // TO remote
                warpDelegate.send("$control " + remoteVar + " " + msg.toString());
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
                        HashUtils.sha1(Mecha.guid(WithCoverage.class)) + 
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
                JSONObject nextMsg = new JSONObject();
                nextMsg.put("$", "next");
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, nextMsg);
            }
        }
        
        /*
         * Forward all control messages upstream.
        */
        public void onControlMessage(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                log.info("<control> "  + proxyVar + ": " + msg.toString());
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
            }
        }
        
        /*
         * Forward all cancel messages upstream.
        */
        public void onCancelEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                log.info("<cancel> "  + proxyVar + ": " + msg.toString());
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
                log.info(proxyVar + ": " + msg.toString());
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
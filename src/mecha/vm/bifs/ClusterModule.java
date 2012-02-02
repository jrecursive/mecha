package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.jinterface.*;
import mecha.util.HashUtils;
import mecha.json.*;
import mecha.vm.*;

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
     * XXX FINISH
    */
    public class Warp extends MVMFunction {
        final String host;
        final String remoteVar;
        
        public Warp(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            host = config.getString("host");
            remoteVar = Mecha.guid(Warp.class);
            /*
             * XXX FINISH
            */
            log.info(config.toString(2));
        }
    }
    
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
         * On start, push start events upstream to generated proxy functions.
        */
        public void onStartEvent(JSONObject msg) throws Exception {
            for(String proxyVar : proxyVars) {
                Mecha.getMVM().nativeControlMessage(getContext(), proxyVar, msg);
            }
        }
        
        /*
         * Forward all data messages.
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

}
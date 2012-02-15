package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;
import mecha.vm.channels.*;
import mecha.vm.flows.*;

public class ClientModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ClientModule.class.getName());

    public ClientModule() throws Exception {
        super();
    }

    public void moduleLoad() throws Exception { }
    
    public void moduleUnload() throws Exception { }
    
    /*
     * Passthru function for var-assigned macro expansion,
     *  e.g., a = (#some-macro ... )
     * Functions as the (final) auto-generated '$sink'
     *  (and origin vertex delegate reference) for the
     *  macro expansion chain.
    */
    public class MacroSink extends MVMFunction {
        public MacroSink(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            broadcastControlMessage(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            broadcastDataMessage(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            broadcastControlMessage(msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            broadcastControlMessage(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("<done> " + msg.toString());
            broadcastDone(msg);
        }
    }
    
    
    /*
     * StartAllSources is a helper function that identifies
     *  all vertices in the current flow which have zero
     *  incoming edges and 1 or more outgoing edges and
     *  sends a start control message to them.
    */
    public class StartAllSources extends MVMFunction {
        public StartAllSources(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            Set<String> sourceRefIds = new HashSet<String>();
            Flow flow = ctx.getFlow();
            for(Vertex v : flow.getVertices()) {
                if (flow.getIncomingEdgesOf(v).size() == 0 &&
                    !v.getRefId().equals(getRefId())) {
                    sourceRefIds.add(v.getRefId());
                }
            }
            JSONObject msg;
            if (config.has("message")) {
                if (config.get("message") instanceof String) {
                    msg = new JSONObject();
                    msg.put("$", config.getString("message"));
                
                } else {
                    msg = config.getJSONObject("message");
                }
            } else {
                msg = new JSONObject();
                msg.put("$", config.getString("start"));
            }
            for(String vertexRefId : sourceRefIds) {
                String controlChannelName = 
                    MVMFunction.deriveControlChannelName(vertexRefId);
                PubChannel channel = Mecha.getChannels().getChannel(controlChannelName);
                if (channel != null) {
                    Mecha.getChannels().getChannel(controlChannelName).send(msg);
                }
            }
        }
    }
    
    /*
     * ClientSink is a generic "sink" function that forwards
     *  any control or data inputs to the calling context's
     *  endpoint (which is, for now, always a websocket connection).
    */
    public class ClientSink extends MVMFunction {
        public ClientSink(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
    
        public void onControlMessage(JSONObject msg) throws Exception {
            msg.put("$type", "control");
            getContext().send(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            msg.put("$type", "data");
            getContext().send(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            msg.put("$type", "start");
            getContext().send(msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            msg.put("$type", "cancel");
            getContext().send(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            msg.put("$type", "done");
            getContext().send(msg);
        }
    }
    
    /*
     * ChannelSink is a sink that directs all messages to a channel
     *  specified on construction.
    */
    public class ChannelSink extends MVMFunction {
        final String channel;
        
        public ChannelSink(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            channel = config.getString("channel");
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            msg.put("$type", "control");
            Mecha.getChannels().getChannel(channel).send(msg);
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            msg.put("$type", "data");
            Mecha.getChannels().getChannel(channel).send(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            msg.put("$type", "start");
            Mecha.getChannels().getChannel(channel).send(msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            msg.put("$type", "cancel");
            Mecha.getChannels().getChannel(channel).send(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            msg.put("$type", "done");
            Mecha.getChannels().getChannel(channel).send(msg);
        }
    }

}
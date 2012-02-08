package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.logging.*;

import org.jetlang.channels.*;
import org.jetlang.core.Callback;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.channels.PubChannel;

public abstract class MVMFunction {
    final private static Logger log = 
        Logger.getLogger(MVMFunction.class.getName());
    
    /*
     * ScriptEngine implements $preprocess and $postprocess
     *  functions universally.
    */
    final private ScriptEngine scriptEngine;
    final private List<String> preprocessFunctions;
    final private List<String> postprocessFunctions;
        
    /*
     * Field in universal MVMFunction message template that
     *  holds the message type (control or data).
    */
    final private static String MESSAGE_TYPE_FIELD = "t";
    
    /*
     * MVMFunction message types.
    */
    final private static String CONTROL_CHANNEL    = "c";
    final private static String DATA_CHANNEL       = "d";
    
    /*
     * Holds the data portion of any message type.
    */
    final private static String OBJECT_FIELD       = "o";
    
    /*
     * MVMFunction refId.
    */
    final private static String ORIGIN_FIELD       = "f";
    
    /*
     * To be returned via overridden getAffinity()
     *  method (or the default value of NO_AFFINITY) for
     *  use by a flow optimizer.
    */
    final private static int NO_AFFINITY        = 0;
    final private static int PRODUCER_AFFINITY  = 1;
    final private static int CONSUMER_AFFINITY  = 2;
    
    /*
     * Context with which the instance of the function was constructed.
    */
    final private MVMContext context;
    
    /*
     * Config passed to constructor.
    */
    final private JSONObject config;
    
    /*
     * refId: a cluster-wide (globally) unique identifier
     *  for this MVMFunction instance.
    */
    final private String refId;
    
    /*
     * incomingChannels: Channels from which this function instance
     *  will receive messages (so as to exert control messages in
     *  response).
     *
     * outgoingChannels: Channels to which all data produced as 
     *  output must be applied as data and/or control messages;
     *  This instance must also respond to control messages originating
     *  _from_ the outgoingChannels.
     *
    */
    final private Set<PubChannel> incomingChannels;
    final private Set<PubChannel> outgoingChannels;
    
    /*
     * Handler for jetlang messages via ProxyChannelConsumer.
    */
    final private static String CONTROL_CHANNEL_SUFFIX = "-c";
    final private Callback<JSONObject> dataChannelCallback;
    final private Callback<JSONObject> controlChannelCallback;
    final private String dataChannelName;
    final private String controlChannelName;
    
    protected MVMFunction() {
        this.context = null;
        this.config = null;
        this.refId = null;
        incomingChannels = null;
        outgoingChannels = null;
        dataChannelName = null;
        dataChannelCallback = null;
        controlChannelName = null;
        controlChannelCallback = null;
        scriptEngine = null;
        preprocessFunctions = null;
        postprocessFunctions = null;
    }
    
    /*
     * Override for additional constructor behavior.
    */
    protected void onCreate(String refId, MVMContext ctx, JSONObject config) throws Exception {
    }
    
    public MVMFunction(String refId,
                       MVMContext context, 
                       JSONObject config) throws Exception {
        this.context = context;
        this.config = config;
        this.refId = refId;
        
        incomingChannels = new HashSet<PubChannel>();
        outgoingChannels = new HashSet<PubChannel>();
        
        dataChannelName = refId;
        dataChannelCallback = new Callback<JSONObject>() {
            public void onMessage(JSONObject message) {
                try {
                    data(message);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
        
        controlChannelName = deriveControlChannelName(refId);
        controlChannelCallback = new Callback<JSONObject>() {
            public void onMessage(JSONObject message) {
                try {
                    control(message);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
        
        /*
         * Scripted pre- and post-processors
        */
        preprocessFunctions = new ArrayList<String>();
        postprocessFunctions = new ArrayList<String>();
        if (config.has("$preprocess") ||
            config.has("$postprocess")) {
            scriptEngine = new ScriptEngine("js");
            scriptEngine.bind("$log", log);
            if (config.has("$preprocess")) {
                if (config.get("$preprocess") instanceof ArrayList) {
                    JSONArray preprocessorNames = config.getJSONArray("$preprocess");
                    for(int i=0; i<preprocessorNames.length(); i++) {
                        preprocessFunctions.add(preprocessorNames.getString(i));
                    }
                } else if (config.get("$preprocess") instanceof String) {
                    preprocessFunctions.add(config.getString("$preprocess"));
                }
            }
            if (config.has("$postprocess")) {
                if (config.get("$postprocess") instanceof ArrayList) {
                    JSONArray postprocessorNames = config.getJSONArray("$postprocess");
                    for(int i=0; i<postprocessorNames.length(); i++) {
                        postprocessFunctions.add(postprocessorNames.getString(i));
                    }
                } else if (config.get("$postprocess") instanceof String) {
                    postprocessFunctions.add(config.getString("$postprocess"));
                }
            }
            
            if (preprocessFunctions.size() > 0) {
                StringBuffer invocationStr = new StringBuffer();
                for(String processorName : preprocessFunctions) {
                    StringBuffer codeBlock = new StringBuffer();
                    for(String line : context.getBlock(processorName)) {
                        codeBlock.append(line);
                        codeBlock.append("\n");
                    }
                    log.info("eval/preprocessor/" + processorName);
                    scriptEngine.eval(codeBlock.toString());
                    invocationStr.append(processorName);
                    invocationStr.append("(");
                }
                invocationStr.append("obj");
                for(int i=0; i<preprocessFunctions.size(); i++) {
                    invocationStr.append(")");
                }
                invocationStr.append(";");
                scriptEngine.eval("var $preprocess = function(obj) { return " + invocationStr.toString() + " };\n");
                log.info("$preprocess registered");
            }
            
            if (postprocessFunctions.size() > 0) {
                StringBuffer invocationStr = new StringBuffer();
                for(String processorName : postprocessFunctions) {
                    StringBuffer codeBlock = new StringBuffer();
                    for(String line : context.getBlock(processorName)) {
                        codeBlock.append(line);
                        codeBlock.append("\n");
                    }
                    log.info("eval/postprocessor/" + processorName);
                    scriptEngine.eval(codeBlock.toString());
                    invocationStr.append(processorName);
                    invocationStr.append("(");
                }
                invocationStr.append("obj");
                for(int i=0; i<postprocessFunctions.size(); i++) {
                    invocationStr.append(")");
                }
                invocationStr.append(";");
                scriptEngine.eval("var $postprocess = function(obj) { return " + invocationStr.toString() + " };\n");
                log.info("$postprocess registered");
            }
            
        } else {
            scriptEngine = null;
        }
        
        onCreate(refId, context, config);
    }
    
    public MVMContext getContext() {
        return context;
    }
    
    public JSONObject getConfig() {
        return config;
    }
    
    public String getRefId() {
        return refId;
    }
    
    /*
     * Channels
    */
    public String getDataChannelName() {
        return dataChannelName;
    }
    
    public String getControlChannelName() {
        return controlChannelName;
    }
    
    public PubChannel getDataChannel() {
        return Mecha.getChannels().getChannel(getDataChannelName());
    }

    public PubChannel getControlChannel() {
        return Mecha.getChannels().getChannel(getControlChannelName());
    }

    protected static String deriveControlChannelName(String dataChannelName) {
        return dataChannelName + CONTROL_CHANNEL_SUFFIX;
    }    
    
    public void addIncomingChannel(PubChannel channel) {
        incomingChannels.add(channel);
    }
    
    public void addOutgoingChannel(PubChannel channel) {
        outgoingChannels.add(channel);
    }
    
    public Set<PubChannel> getIncomingChannels() {
        return incomingChannels;
    }
    
    public Set<PubChannel> getOutgoingChannels() {
        return outgoingChannels;
    }
    
    /*
     * Preprocess an outgoing data message (via 
     *  $preprocess directive).
    */
    public JSONObject preprocessDataMessage(JSONObject obj) throws Exception {
        if (scriptEngine == null || preprocessFunctions.size() == 0) return obj;
        return (JSONObject) scriptEngine.invoke("$preprocess", obj);
    }
    
    /*
     * Postprocess an outgoing data message (via 
     *  $postprocess directive).
    */
    public JSONObject postprocessDataMessage(JSONObject obj) throws Exception {
        if (scriptEngine == null || postprocessFunctions.size() == 0) return obj;
        return (JSONObject) scriptEngine.invoke("$postprocess", obj);
    }
    
    /*
     * Broadcast a data message to all outgoingChannels.
    */
    public void broadcastDataMessage(JSONObject msg) throws Exception {
        msg.put("$origin", getRefId());
        msg = postprocessDataMessage(msg);
        for(PubChannel channel : outgoingChannels) {
            channel.send(msg);
        }
    }
    
    /*
     * Broadcast a control message to all outgoingChannels.
    */
    public void broadcastControlMessage(JSONObject msg) throws Exception {
        msg.put("$origin", getRefId());
        for(PubChannel channel : outgoingChannels) {
            PubChannel dataChannel = 
                Mecha.getChannels().getChannel(
                    deriveControlChannelName(channel.getName()));
            dataChannel.send(msg);
        }
    }
    
    /*
     * Broadcast a control message to all incomingChannels.
    */
    public void broadcastControlMessageUpstream(JSONObject msg) throws Exception {
        msg.put("$origin", getRefId());
        for(PubChannel channel : incomingChannels) {
            PubChannel dataChannel = 
                Mecha.getChannels().getChannel(
                    deriveControlChannelName(channel.getName()));
            dataChannel.send(msg);
        }
    }

    public void broadcastDone() throws Exception {
        broadcastDone(new JSONObject());
    }
    
    public void broadcastDoneUpstream() throws Exception {
        broadcastDoneUpstream(new JSONObject());
    }
    
    public void broadcastDone(JSONObject msg) throws Exception {
        msg.put("$", "done");
        msg.put("$type", "done");
        broadcastControlMessage(msg);
    }
    
    public void broadcastDoneUpstream(JSONObject msg) throws Exception {
        broadcastControlMessageUpstream(msg);
    }

    /*
     * Send a data message to a specific channel.
    */
    public void sendData(String channel, JSONObject obj) throws Exception {
        PubChannel dataChannel = Mecha.getChannels().getChannel(channel);
        obj.put("$origin", getRefId());
        obj = postprocessDataMessage(obj);
        dataChannel.send(obj);
    }
        
    /*
     * Send a control message to a specific channel.
     *
     * channel: assumes data channel name (automatically
     *  derives control channel name).
    */
    public void sendControl(String channel, JSONObject obj) throws Exception {
        PubChannel controlChannel = Mecha.getChannels().getChannel(deriveControlChannelName(channel));
        obj.put("$origin", getRefId());
        controlChannel.send(obj);
    }
    
    /*
     * Post-assignnment event.
    */
    public void onPostAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
    }
    
    public void postAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
        onPostAssignment(ctx, var, ast);
    }    
    
    /*
     * Jetlang message handlers.
    */
    protected Callback<JSONObject> getDataChannelCallback() throws Exception {
        return dataChannelCallback;
    }
    
    protected Callback<JSONObject> getControlChannelCallback() throws Exception {
        return controlChannelCallback;
    }
    
    /* 
     * Override control & data to handle each
     *  message type.
    */
    public void onControlMessage(JSONObject msg) throws Exception {
    }
    
    public void control(JSONObject msg) throws Exception {
        /*
         * detect & intercept 'native' control messages:
         *  start
         *  cancel
         *  done
         *  ping
        */
        String cmd = msg.getString("$");
        if (cmd.equals("start")) {
            start(msg);
        
        } else if (cmd.equals("cancel")) {
            cancel(msg);
        
        } else if (cmd.equals("done")) {
            done(msg);
        
        } else if (cmd.equals("ping")) {
            log.info("pong");
        
        } else {
            onControlMessage(msg);
        }
    }
    
    public void onDataMessage(JSONObject msg) throws Exception {
    }
    
    public void data(JSONObject msg) throws Exception {
        msg = preprocessDataMessage(msg);
        onDataMessage(msg);
    }
    
    /*
     * Override start & cancel to handle "baked-in" control
     *  operations.
    */
    
    public void onStartEvent(JSONObject msg) throws Exception {
    }
    
    public void start(JSONObject msg) throws Exception {
        onStartEvent(msg);
    }
    
    public void onCancelEvent(JSONObject msg) throws Exception {
    }
    
    public void cancel(JSONObject msg) throws Exception {
        onCancelEvent(msg);
    }
    
    public void onDoneEvent(JSONObject msg) throws Exception {
        /*
         * Default behavior is to forward "done" messages.
        */
        
        /*
         * NOTE: should we automatically destroy the Jetlang
         *       fiber here?
        */
        broadcastDone(msg);
    }
    
    public void done(JSONObject msg) throws Exception {
        onDoneEvent(msg);
    }
    
    /*
     * Place & route helpers
    */
    
    /*
     * Override this function to provide a different
     *  affinity value to be used by a flow optimizer.
    */
    public int getAffinity() {
        return NO_AFFINITY;
    }    

}

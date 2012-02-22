package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.logging.*;

import org.jetlang.channels.*;
import org.jetlang.core.Callback;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.channels.PubChannel;
import mecha.vm.flows.*;
import mecha.monitoring.*;

public abstract class MVMFunction {
    final private static Logger log = 
        Logger.getLogger(MVMFunction.class.getName());
    
    final static private Rates rates = new Rates();
    static {
        Mecha.getMonitoring().addMonitoredRates(rates);
    }
    
    /*
     * ScriptEngine implements $preprocess and $postprocess
     *  functions universally.
    */
    final private ScriptEngine scriptEngine;
    final private List<String> preprocessFunctions;
    final private List<String> postprocessFunctions;
    final private Map<String, JSONObject> preprocessConfig;
    final private Map<String, JSONObject> postprocessConfig;
        
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
    
    private boolean isDone;
    
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
        preprocessConfig = null;
        postprocessFunctions = null;
        postprocessConfig = null;
        isDone = true;
    }
    
    /*
     * Override for additional constructor behavior.
    */
    protected void onCreate(String refId, MVMContext ctx, JSONObject config) throws Exception {
    }
    
    public MVMFunction(String refId,
                       MVMContext context, 
                       JSONObject config) throws Exception {
        incomingChannels = new HashSet<PubChannel>();
        outgoingChannels = new HashSet<PubChannel>();
        this.context = context;
        this.config = config;
        this.refId = refId;
        isDone = false;
        
        dataChannelName = refId;
        dataChannelCallback = new Callback<JSONObject>() {
            public void onMessage(JSONObject message) {
                try {
                    data(message);
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.vm.mvm-function", ex);
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
                    Mecha.getMonitoring().error("mecha.vm.mvm-function", ex);
                    ex.printStackTrace();
                }
            }
        };
        
        /*
         * Scripted pre- and post-processors
        */
        preprocessFunctions = new ArrayList<String>();
        preprocessConfig = new HashMap<String, JSONObject>();
        
        postprocessFunctions = new ArrayList<String>();
        postprocessConfig = new HashMap<String, JSONObject>();
        
        if (config.has("$preprocess") ||
            config.has("$postprocess")) {
            scriptEngine = new ScriptEngine("js");
            scriptEngine.bind("$log", 
                              Logger.getLogger(
                                config.<String>get("$") + "-" + 
                                "data-processor"));
            scriptEngine.bind("$mecha", Mecha.get());
            scriptEngine.bind("$enclosing_function", getConfig());
            
            if (config.has("$preprocess")) {
                interpretPrePostConfig(preprocessFunctions, 
                                       preprocessConfig, 
                                       config,
                                       "$preprocess");
            }
            if (config.has("$postprocess")) {
                interpretPrePostConfig(postprocessFunctions, 
                                       postprocessConfig, 
                                       config,
                                       "$postprocess");
            }
        } else {
            scriptEngine = null;
        }

        onCreate(refId, context, config);
    }
    
    private void interpretPrePostConfig(List<String> processorFunctionList, 
                                        Map<String, JSONObject> processorFunctionConfig,
                                        JSONObject config, 
                                        String field) throws Exception {
        // list of preprocessors without arguments, e.g,. (a b c d)
        if (config.get(field) instanceof ArrayList) {
            JSONArray processorNames = config.getJSONArray(field);
            for(int i=0; i<processorNames.length(); i++) {
                processorFunctionConfig.put(processorNames.getString(i), new JSONObject());
                processorFunctionList.add(processorNames.getString(i));
            }
        
        // a single preprocessor without arguments, e.g., (a)
        } else if (config.get(field) instanceof String) {
            processorFunctionConfig.put(config.getString(field), new JSONObject());
            processorFunctionList.add(config.getString(field));
        
        // one or more processors with a potential mix of no arguments &
        //  specified arguments.
        } else if (config.get(field) instanceof Map) {
            JSONObject processorConfig = config.getJSONObject(field);
            for(String f : JSONObject.getNames(processorConfig)) {
                if (f.equals("$")) {
                    if (processorConfig.get("$") instanceof String) {
                        processorFunctionConfig.put(processorConfig.<String>get("$"), 
                                                    new JSONObject());
                        processorFunctionList.add(processorConfig.<String>get("$"));
                    
                    // can only be a list otherwise
                    } else {
                        for(String processorName : processorConfig.<List<String>>get("$")) {
                            processorFunctionConfig.put(processorName, new JSONObject());
                            processorFunctionList.add(processorName);
                        }
                    }
                
                // processors with specified configuration objects
                } else {
                    processorFunctionConfig.put(f, processorConfig.getJSONObject(f));
                    processorFunctionList.add(f);
                }
            }
        }
        
        /* 
         * set up script engine with config object bindings, 
         *  processors (as functions) and create & bind the
         *  composed function of all processors.
        */
        if (processorFunctionList.size() > 0) {
            StringBuffer invocationStr = new StringBuffer();
            
            /*
             * pre-bind config in case pre-function definition
             *  code utilizes function config options.
            */
            for(String processorName : processorFunctionList) {
                scriptEngine.bind(processorName + "Config",
                                  processorFunctionConfig.get(processorName));
            }
            for(String processorName : processorFunctionList) {
                StringBuffer codeBlock = new StringBuffer();
                for(String line : context.getBlock(processorName)) {
                    codeBlock.append(line);
                    codeBlock.append("\n");
                }
                log.info("processor setup: eval/" + field + "/" + processorName);
                scriptEngine.eval(codeBlock.toString());
                invocationStr.append(processorName);
                invocationStr.append("(");
            }
            invocationStr.append("obj");
            for(int i=0; i<processorFunctionList.size(); i++) {
                invocationStr.append(")");
            }
            invocationStr.append(";");
            scriptEngine.eval(
                "var " + field + " = " +
                    "function(obj) { " +
                        "return " + invocationStr.toString() + 
                    " };\n");
            log.info(field + " registered");
        }
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
        if (!msg.has("$type")) {
            msg = postprocessDataMessage(msg);
        }
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

    public void broadcastDone() {
        broadcastDone(new JSONObject());
    }
    
    public void broadcastDoneUpstream() throws Exception {
        broadcastDoneUpstream(new JSONObject());
    }
    
    public void broadcastDone(JSONObject msg) {
        try {
            msg.put("$", "done");
            msg.put("$type", "done");
            broadcastDataMessage(msg);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.mvm-function", ex);
            ex.printStackTrace();
        }
    }
    
    public void broadcastDoneUpstream(JSONObject msg) throws Exception {
        msg.put("$origin", getRefId());
        for(PubChannel dataChannel : incomingChannels) {
            dataChannel.send(msg);
        }
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
        rates.add("mecha.vm.mvm-function.global.control-message");
        
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
        
        } else if (cmd.equals("ping")) {
            log.info("pong");
        
        } else {
            onControlMessage(msg);
        }
    }
    
    public void onDataMessage(JSONObject msg) throws Exception {
    }
    
    public void data(JSONObject msg) throws Exception {
        rates.add("mecha.vm.mvm-function.global.data-message");
        if (msg.has("$") &&
            msg.getString("$").equals("done")) {
            done(msg);
        } else {
            msg = preprocessDataMessage(msg);
            onDataMessage(msg);
        }
    }
    
    /*
     * Override start & cancel to handle "baked-in" control
     *  operations.
    */
    
    public void onStartEvent(JSONObject msg) throws Exception {
    }
    
    public void start(JSONObject msg) throws Exception {
        rates.add("mecha.vm.mvm-function.global.start-message");
        onStartEvent(msg);
    }
    
    public void onCancelEvent(JSONObject msg) throws Exception {
    }
    
    public void cancel(JSONObject msg) throws Exception {
        rates.add("mecha.vm.mvm-function.global.cancel-message");
        onCancelEvent(msg);
    }
    
    public void onDoneEvent(JSONObject msg) throws Exception {
        /*
         * Default behavior is to forward "done" messages downstream.
        */
        broadcastDone(msg);
    }
    
    public void done(JSONObject msg) throws Exception {
        rates.add("mecha.vm.mvm-function.global.done-message");
        /*
        if (isDone) {
            throw new Exception("MVMFunction already called done!");
        }
        */
        isDone = true;
        onDoneEvent(msg);
    }
    
    /*
     * Channel cleanup.
    */
    protected void releaseChannels() throws Exception {
        Set<String> releasableChannels = new HashSet<String>();
        for (PubChannel channel : incomingChannels) {
            releasableChannels.add(channel.getName());
        }
        for (PubChannel channel : outgoingChannels) {
            releasableChannels.add(channel.getName());
        }
        for (String channelName : releasableChannels) {
            try {
                Mecha.getChannels().destroyChannel(channelName);
                Mecha.getChannels().destroyChannel(channelName + 
                    "-" + CONTROL_CHANNEL);
            } catch (Exception ex) {
                // gulp.
            }
        }
        incomingChannels.clear();
        outgoingChannels.clear();
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

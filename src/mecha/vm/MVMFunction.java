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
    }
    
    protected void onCreate(String refId, MVMContext ctx, JSONObject config) throws Exception {
        //log.info("onCreate()");
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
        
        onCreate(refId, context, config);
        
        //log.info("<constructor> " + this.getClass().getName());
    }
    
    public MVMContext getContext() {
        return context;
    }
    
    public JSONObject getConfig() {
        return config;
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
     * Broadcast a data message to all outgoingChannels.
    */
    public void broadcastDataMessage(JSONObject msg) throws Exception {
        for(PubChannel channel : outgoingChannels) {
            channel.send(msg);
        }
    }
    
    /*
     * Broadcast a control message to all outgoingChannels.
    */
    public void broadcastControlMessage(JSONObject msg) throws Exception {
        for(PubChannel channel : outgoingChannels) {
            PubChannel dataChannel = 
                Mecha.getChannels().getChannel(
                    deriveControlChannelName(channel.getName()));
            dataChannel.send(msg);
        }
    }

    public void broadcastDone() throws Exception {
        JSONObject msg = new JSONObject();
        broadcastDone(msg);
    }
    
    public void broadcastDone(JSONObject msg) throws Exception {
        msg.put("$", "done");
        broadcastControlMessage(msg);
    }

    /*
     * Send a data message to a specific channel.
    */
    public void sendData(String channel, JSONObject obj) throws Exception {
        PubChannel dataChannel = Mecha.getChannels().getChannel(channel);
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
        controlChannel.send(obj);
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
        //log.info("generic onControlMessage: msg = " + msg.toString(2));
    }
    
    public void control(JSONObject msg) throws Exception {
        /*
         * detect & intercept 'native' control messages:
         *  start
         *  cancel
         *  done
        */
        //log.info("<control> " + this.getClass().getName() + ": " +
        //    msg.toString(2));
        String cmd = msg.getString("$");
        if (cmd.equals("start")) {
            start(msg);
        
        } else if (cmd.equals("cancel")) {
            cancel(msg);
        
        } else if (cmd.equals("done")) {
            done(msg);
        
        } else {
            onControlMessage(msg);
        }
    }
    
    public void onDataMessage(JSONObject msg) throws Exception {
        //log.info("generic onDataMessage: msg = " + msg.toString(2));
    }
    
    public void data(JSONObject msg) throws Exception {
        //log.info("<data> " + this.getClass().getName() + ": " +
        //    msg.toString(2));
        onDataMessage(msg);
    }
    
    /*
     * Override start & cancel to handle "baked-in" control
     *  operations.
    */
    
    public void onStartEvent(JSONObject msg) throws Exception {
        //log.info("generic onStartEvent: msg = " + msg.toString(2));
    }
    
    public void start(JSONObject msg) throws Exception {
        //log.info("start: " + msg.toString(2));
        onStartEvent(msg);
    }
    
    public void onCancelEvent(JSONObject msg) throws Exception {
        //log.info("generic onCancelEvent: msg = " + msg.toString(2));
    }
    
    public void cancel(JSONObject msg) throws Exception {
        //log.info("cancel: " + msg.toString(2));
        onCancelEvent(msg);
    }
    
    public void onDoneEvent(JSONObject msg) throws Exception {
        //log.info("generic onDoneEvent: msg = " + msg.toString(2));
    }
    
    public void done(JSONObject msg) throws Exception {
        //log.info("done: " + msg.toString(2));
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

package mecha.vm;

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
    
    public MVMFunction(String refId,
                       MVMContext context, 
                       JSONObject config) throws Exception {
        this.context = context;
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
                    data(message);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
                
        log.info("<constructor> " + this.getClass().getName());
    }
    
    public MVMContext getContext() {
        return context;
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
        incomingChannels.add(channel);
    }
    
    public Set<PubChannel> getIncomingChannels() {
        return incomingChannels;
    }
    
    public Set<PubChannel> getOutgoingChannels() {
        return outgoingChannels;
    }
    
    /*
     * Broadcast a message to all outgoingChannels.
    */
    public void sendData(JSONObject msg) throws Exception {
        for(PubChannel channel : outgoingChannels) {
            channel.send(msg);
        }
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
     * Override control, data, and info to handle each
     *  message type.
    */
    public void control(JSONObject msg) throws Exception {
        log.info("<control> " + this.getClass().getName() + ": " +
            msg.toString(2));
    }

    public void data(JSONObject msg) throws Exception {
        log.info("<data> " + this.getClass().getName() + ": " +
            msg.toString(2));
    }

    public void info(JSONObject msg) throws Exception {
        log.info("info: " + msg.toString(2));
    }
    
    /*
     * Scheduling and process management
    */
    
    /*
     * Override this function to provide a different
     *  affinity value to be used by a flow optimizer.
    */
    public int getAffinity() {
        return NO_AFFINITY;
    }    

}

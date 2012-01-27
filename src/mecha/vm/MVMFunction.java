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
    final private Callback<JSONObject> proxyChannelCallback;
    
    public MVMFunction(String refId,
                       MVMContext context, 
                       JSONObject config) throws Exception {
        this.context = context;
        this.refId = refId;
        
        incomingChannels = new HashSet<PubChannel>();
        outgoingChannels = new HashSet<PubChannel>();
        
        proxyChannelCallback = new Callback<JSONObject>() {
            public void onMessage(JSONObject message) {
                try {
                    if (message.has(MESSAGE_TYPE_FIELD)) {
                        if (message.getString(MESSAGE_TYPE_FIELD)
                                   .equals(CONTROL_CHANNEL)) {
                            control(message.getJSONObject(OBJECT_FIELD));
                        } else if (message.getString(MESSAGE_TYPE_FIELD)
                                          .equals(DATA_CHANNEL)) {
                            data(message.getJSONObject(OBJECT_FIELD));
                        } else {
                            info(message);
                        }
                    } else {
                        info(message);
                    }
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
    public void sendData(JSONObject obj) throws Exception {
        JSONObject msg = newDataMessage(obj);
        for(PubChannel channel : outgoingChannels) {
            channel.send(msg);
        }
    }
    
    /*
     * Send a control message to a specific channel.
    */
    public void sendControl(String channel, JSONObject obj) throws Exception {
        sendControl(Mecha.getChannels().getChannel(channel), obj);
    }
    
    public void sendControl(PubChannel channel, JSONObject obj) throws Exception {
        JSONObject msg = newControlMessage(obj);
        channel.send(msg);
    }
    
    /*
     * Create a data message.
    */
    public JSONObject newDataMessage(JSONObject obj) throws Exception {
        JSONObject msg = newBaseFunctionMessage();
        msg.put(MESSAGE_TYPE_FIELD, DATA_CHANNEL);
        msg.put(OBJECT_FIELD, obj);
        return msg;
    }
    
    /*
     * Create a control message.
    */
    public JSONObject newControlMessage(JSONObject obj) throws Exception {
        JSONObject msg = newBaseFunctionMessage();
        msg.put(MESSAGE_TYPE_FIELD, CONTROL_CHANNEL);
        msg.put(OBJECT_FIELD, obj);
        return msg;
    }
    
    /*
     * Create a MVMFunction universal message template.
    */
    public JSONObject newBaseFunctionMessage() throws Exception {
        JSONObject msg = new JSONObject();
        msg.put(ORIGIN_FIELD, refId);
        return msg;
    }
            
    /*
     * Get jetlang message handler.
    */
    protected Callback<JSONObject> getCallback() throws Exception {
        return proxyChannelCallback;
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

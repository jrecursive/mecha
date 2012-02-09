package mecha.server;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.jboss.netty.channel.ChannelHandlerContext;

import mecha.Mecha;
import mecha.util.*;
import mecha.vm.*;
import mecha.vm.channels.*;
import mecha.json.*;

/*
 * client record
*/
public class Client implements ChannelConsumer {
    final private static Logger log = 
        Logger.getLogger(Client.class.getName());

    /*
     * for Server
    */
    final private ConcurrentHashMap<String, String> state;
    private boolean authorized = false;
    
    /*
     * Blocks.
    */
    private boolean withinBlock = false;
    private boolean withinGlobalBlock = false;
    private List<String> block;
    private String blockName;
    
    /*
     * messaging via mecha.vm.channels.Channels
    */
    final private Set<String> subscriptions;
    final private WeakReference<ChannelHandlerContext> connection;
    final private PubChannel clientChannel;
    
    /*
     * Mecha VM
    */
    final private MVMContext ctx;
    
    /*
     * per-client identifiers
    */
    final private String id;
    
    public Client(ChannelHandlerContext connection) throws Exception {
        id = "socket-" +
             HashUtils.sha1(
                UUID.randomUUID() + "-" +
                System.currentTimeMillis()
             );
        log.info("new client: " + id);
    
        state = new ConcurrentHashMap<String, String>();
        this.connection = new WeakReference<ChannelHandlerContext>(connection);
        subscriptions = Collections.synchronizedSet(new HashSet());
        
        block = new ArrayList<String>();
        blockName = null;
        
        /*
         * MVMContext keeps WeakReference<Client>
        */
        ctx = new MVMContext(this);
        
        /*
         * subscribe to own channel
        */
        clientChannel = Mecha.getChannels().getOrCreateChannel(id);
        clientChannel.addMember(this);
        addSubscription(id);
    }
    
    /*
     * for Server
    */
    
    public void setAuthorized(boolean authorized) {
        this.authorized = authorized;
    }
    
    public boolean isAuthorized() {
        return authorized;
    }
    
    public ConcurrentHashMap<String, String> getState() {
        return state;
    }
    
    /*
     * Block definition
    */
    
    public String getBlockName() {
        return blockName;
    }
    
    public void setBlockName(String blockName) {
        this.blockName = blockName;
    }
    
    public boolean withinBlock() {
        return withinBlock;
    }
    
    public boolean withinGlobalBlock() {
        return withinGlobalBlock;
    }
    
    public void setWithinBlock(boolean isWithinBlock) {
        withinBlock = isWithinBlock;
    }
    
    public void setWithinGlobalBlock(boolean isWithinGlobalBlock) {
        withinGlobalBlock = isWithinGlobalBlock;
    }
    
    public void clearBlock() {
        block = new ArrayList<String>();
    }
    
    public void appendBlock(String line) {
        block.add(line);
    }
    
    public List<String> getBlock() {
        return block;
    }
    
    /* 
     * messaging
    */
    
    public Set<String> getSubscriptions() {
        return subscriptions;
    }
    
    public void addSubscription(String channel) {
        subscriptions.add(channel);
    }
    
    public void removeSubscription(String channel) {
        subscriptions.remove(channel);
    }
    
    public PubChannel getChannel() {
        return clientChannel;
    }
    
    /*
     * MVM support
    */
    
    public MVMContext getContext() {
        return ctx;
    }
    
    /*
     * Misc
    */
    
    public String getId() {
        return id;
    }
    
    public ChannelHandlerContext getConnection() {
        return connection.get();
    }
    
    /*
     * implementation of ChannelConsumer
    */
    
    public void onMessage(String channel, String message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("c", channel);
        messageObj.put("o", message);
        send(messageObj.toString());
    }
    
    public void onMessage(String channel, JSONObject message) throws Exception {
        JSONObject messageObj = new JSONObject();
        messageObj.put("c", channel);
        messageObj.put("o", message);
	    send(messageObj.toString());
    }
    
    /*
     * the byte-based channel requires the sender to
     *  have an implicit understanding with the receivers
    */
    public void onMessage(String channel, byte[] message) throws Exception {
        send(message);
    }
    
    public void send(String message) throws Exception {
        connection.get().getChannel().write(message + "\n").awaitUninterruptibly();
    }
    
    public void send(byte[] message) throws Exception {
        send(new String(message));
    }
}
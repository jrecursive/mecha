package mecha.vm;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.jetlang.channels.*;
import org.jetlang.core.*;
import org.jetlang.fibers.*;

import mecha.Mecha;
import mecha.server.*;
import mecha.json.*;
import mecha.vm.*;
import mecha.vm.flows.*;
import mecha.vm.channels.*;

public class MVMContext {
    final private static Logger log = 
        Logger.getLogger(MVMContext.class.getName());

    final private ConcurrentHashMap<String, Object> vars;
    
    /*
     * Maps vertexRefId values to a weak reference to an MVMFunction instance.
    */
    final private ConcurrentHashMap<String, WeakReference<MVMFunction>> funRefs;
    
    final private WeakReference<Client> clientRef;
    
    /*
     * Jetlang components & management
    */
    private ExecutorService functionExecutor;
    private PoolFiberFactory fiberFactory;
    private ConcurrentHashMap<String, Channel<JSONObject>> memoryChannelMap;
    private Set<Fiber> fibers;
    
    /*
     * Execution context
    */
    private Flow flow;
    private String refId;

    public MVMContext(Client client) throws Exception {
        clientRef = new WeakReference<Client>(client);
        vars = new ConcurrentHashMap<String, Object>();
        funRefs = new ConcurrentHashMap<String, WeakReference<MVMFunction>>();
        flow = new Flow();
        refId = Mecha.guid(MVMContext.class);
        
        functionExecutor = Executors.newCachedThreadPool();
        fiberFactory = new PoolFiberFactory(functionExecutor);
        memoryChannelMap = new ConcurrentHashMap<String, Channel<JSONObject>>();
        fibers = new HashSet<Fiber>();
    }
    
    /*
     * MVM context-scope variables
    */
    
    public ConcurrentHashMap<String, Object> getVars() {
        return vars;
    }
    
    public <T> T get(String key) {
        return (T)vars.get(key);
    }
        
    public void put(String key, Object val) {
        vars.put(key, val);
    }
    
    public void remove(String key) {
        vars.remove(key);
    }
    
    public void clearVars() {
        vars.clear();
    }
    
    public MVMFunction getFunRef(String vertexRefId) throws Exception {
        return funRefs.get(vertexRefId).get();
    }
    
    public void putFunRef(String vertexRefId, MVMFunction inst) throws Exception {
        funRefs.put(vertexRefId, new WeakReference<MVMFunction>(inst));
    }
    
    public void clearFunRefs() {
        funRefs.clear();
    }
    
    /*
     * flow
    */
    
    public void clearFlow() {
        flow = new Flow();
    }
    
    public Flow getFlow() {
        return flow;
    }
    
    /*
     * helpers
    */
    
    public Client getClient() {
        return clientRef.get();
    }
    
    public String getClientId() {
        return clientRef.get().getId();
    }
    
    public String getRefId() {
        return refId;
    }
    
    public String resolveAssignmentToRefId(String var) throws Exception {
        return this.<String>get(var);
    }
    
    /*
     * Context-specific messaging
    */
    
    public void send(JSONObject msg) throws Exception {
        getClient().getChannel().send(msg);
        //log.info(msg.toString(2));
    }
    
    /*
     * Jetlang helpers
    */
    
    private Fiber newFiber() throws Exception {
        Fiber fiber = fiberFactory.create();
        fibers.add(fiber);
        fiber.start();
        return fiber;
    }
    
    private Channel<JSONObject> newJetlangChannel(String refId) throws Exception {
        Channel<JSONObject> jetlangChannel = 
            new MemoryChannel<JSONObject>();
        memoryChannelMap.put(refId, jetlangChannel);
        return jetlangChannel;
    }
    
    private void
        newJetlangChannelConsumerProxy(final String mechaChannelName, 
                                       final Fiber fiber,
                                       final Channel<JSONObject> jetlangChannel,
                                       final Callback<JSONObject> jetlangCallback) throws Exception {
        ChannelConsumer proxyChannelConsumer = new ChannelConsumer() {
            public void onMessage(String channel, String message) throws Exception {
                log.info("this should never happen!  String message = " + message);
            }
    
            public void onMessage(String channel, JSONObject message) throws Exception {
                //log.info(channel + " -> " + message.toString(2));
                if (!channel.equals(mechaChannelName)) {
                    log.info("!! " + channel + " != " + mechaChannelName);
                }
                jetlangChannel.publish(message);
            }
        
            public void onMessage(String channel, byte[] message) throws Exception {
                log.info("this should never happen!  byte[] message = " + new String(message));
            }
        };
        Mecha.getChannels()
             .getOrCreateChannel(mechaChannelName)
             .addMember(proxyChannelConsumer);
        jetlangChannel.subscribe(fiber, jetlangCallback);
    }
    
    public void startFunctionTask(String vertexRefId, MVMFunction inst)
        throws Exception {
        
        /*
         * Data Channel
        */
        final Fiber dataFiber = newFiber();
        final Channel<JSONObject> jetlangDataChannel = newJetlangChannel(vertexRefId);
        newJetlangChannelConsumerProxy(vertexRefId, 
                                       dataFiber,
                                       jetlangDataChannel,
                                       inst.getDataChannelCallback());
        /*
         * Control channel
        */
        final String controlChannelName = MVMFunction.deriveControlChannelName(vertexRefId);
        final Fiber controlFiber = newFiber();
        final Channel<JSONObject> jetlangControlChannel = 
            newJetlangChannel(controlChannelName);
        newJetlangChannelConsumerProxy(controlChannelName, 
                                       dataFiber,
                                       jetlangControlChannel,
                                       inst.getControlChannelCallback());
    }
    
    /*
     * Jetlang debug helpers
    */
    
    protected ConcurrentHashMap<String, Channel<JSONObject>> getMemoryChannelMap() {
        return memoryChannelMap;
    }
    
    /*
     * Jetlang reset helpers
    */
    
    private void disposeFibers() throws Exception {
        for(Fiber fiber : fibers) {
            log.info("fiber.dispose: " + fiber);
            fiber.dispose();
        }
    }
    
    private void clearChannels() throws Exception {
        memoryChannelMap.clear();
    }
    
    private void shutdownFunctionExecutor() throws Exception {
        List<Runnable> waitingTasks = 
            functionExecutor.shutdownNow();
        for(Runnable r : waitingTasks) {
            log.info("executor: killing waiting task: " + r);
            if (r instanceof Thread) {
                log.info("interrupting: " + r);
                ((Thread)r).interrupt();
            }
        }
        log.info("isTerminated: " + functionExecutor.isTerminated());
        functionExecutor = null;
    }
    
    private void resetJetlangPrimitives() throws Exception {
        disposeFibers();
        clearChannels();
        shutdownFunctionExecutor();

        functionExecutor = Executors.newCachedThreadPool();
        fiberFactory = new PoolFiberFactory(functionExecutor);
        memoryChannelMap = new ConcurrentHashMap<String, Channel<JSONObject>>();
        fibers = new HashSet<Fiber>();
    }
    
    /*
     * Clear all assignments (vars), reset jetlang primitives and create a new empty flow.
    */
    public void reset() throws Exception {
        resetJetlangPrimitives();
        clearVars();
        clearFunRefs();
        clearFlow();
    }
    
}
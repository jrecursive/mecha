/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
    
    /*
     * Macro expansion "vertex delegate" map; maps
     *  "reference variable" (e.g., "a") to the
     *  "true destination" (ultimately, the macro's
     *  generated (guid) '$sink'.
    */
    final private ConcurrentHashMap<String, String> vertexDelegates;
    
    /*
     * Primitive function assignment -> ref maps.
    */
    final private ConcurrentHashMap<String, Object> vars;
    
    /*
     * Maps vertexRefId values to a weak reference to an MVMFunction instance.
    */
    final private ConcurrentHashMap<String, WeakReference<MVMFunction>> funRefs;
    
    final private WeakReference<Client> clientRef;
    
    /*
     * Maps a "block name" to a "block" (list of strings)
    */
    final private ConcurrentHashMap<String, List<String>> blocks;
    
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
        blocks = new ConcurrentHashMap<String, List<String>>();
        
        functionExecutor = newExecutorService();
        fiberFactory = new PoolFiberFactory(functionExecutor);
        memoryChannelMap = new ConcurrentHashMap<String, Channel<JSONObject>>();
        fibers = new HashSet<Fiber>();
        
        vertexDelegates = new ConcurrentHashMap<String, String>();
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
        for (String refId : funRefs.keySet()) {
            WeakReference<MVMFunction> funRef = funRefs.get(refId);
            MVMFunction fun = funRef.get();
            if (fun != null) {
                try {
                    fun.releaseChannels();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    Mecha.getMonitoring().error("mecha.mvm-context", ex);
                }
            }
        }
        funRefs.clear();
    }
    
    /* 
     * Resolve destination vertex variables to a delegate if one is defined,
     *  or itself if not.
    */
    public String resolveVertexDelegate(String var) throws Exception {
        if (vertexDelegates.containsKey(var)) {
            /*
            log.info("resolveVertexDelegate(" + var + ") -> " + 
                vertexDelegates.get(var));
            */
            return vertexDelegates.get(var);
        } else {
            return var;
        }
    }
    
    public void setVertexDelegate(String var, String delegateVar) throws Exception {
        vertexDelegates.put(var, delegateVar);
    }
    
    public void clearVertexDelegates() {
        vertexDelegates.clear();
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
     * blocks
    */
    
    public void clearBlocks() {
        blocks.clear();
    }
    
    public void setBlock(String blockName, List<String> block) {
        blocks.put(blockName, block);
    }
    
    public void removeBlock(String blockName) {
        blocks.remove(blockName);
    }
    
    public List<String> getBlock(String blockName) {
        return blocks.get(blockName);
    }
    
    public ConcurrentHashMap<String, List<String>> getBlockMap() {
        return blocks;
    }
    
    /*
     * Context-specific messaging
    */
    
    public void send(JSONObject msg) throws Exception {
        /*
         * Send message.
        */
        if (msg != null &&
            getClient() != null &&
            getClient().getChannel() != null) {
            getClient().getChannel().send(msg);
        }
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
        
        if (functionExecutor.isShutdown()) {
            throw new Exception("function executor has been shut down, try again");
        }
        
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
    
    private void cancelAllFunctions(String reason) throws Exception {
        for(String refId : funRefs.keySet()) {
            WeakReference<MVMFunction> funRef = funRefs.get(refId);
            if (funRef != null) {
                MVMFunction fun = funRef.get();
                if (fun != null) {
                    if (!reason.equals("reset")) {
                        log.info("cancel: " + refId + ": reason: " + reason);
                    }
                    JSONObject cancelMsg = new JSONObject();
                    cancelMsg.put("$", "cancel");
                    cancelMsg.put("reason", reason);
                    fun.cancel(cancelMsg);
                } else {
                    log.info("cancel: " + refId + ": void reference (already dead)");
                }
            }
        }
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
            //log.info("fiber.dispose: " + fiber);
            fiber.dispose();
        }
    }
    
    private void clearChannels() throws Exception {
        memoryChannelMap.clear();
    }
    
    private void shutdownFunctionExecutor() throws Exception {
        functionExecutor.shutdownNow();
        while(!functionExecutor.isTerminated()) {
            List<Runnable> waitingTasks = 
                functionExecutor.shutdownNow();
            if (waitingTasks.size() == 0) break;
            log.info(waitingTasks.size() + " waiting tasks: ");
            for(Runnable r : waitingTasks) {
                log.info("task: " + r.toString() + " <" + r.getClass().getName() + ">");
                if (r instanceof Thread) {
                    log.info("interrupting: " + r);
                    ((Thread)r).interrupt();
                }
            }
            Thread.sleep(1000);
        }
        functionExecutor = null;
    }
    
    private void resetJetlangPrimitives() throws Exception {
        disposeFibers();
        clearChannels();
        shutdownFunctionExecutor();

        functionExecutor = newExecutorService();
        fiberFactory = new PoolFiberFactory(functionExecutor);
        memoryChannelMap = new ConcurrentHashMap<String, Channel<JSONObject>>();
        fibers = new HashSet<Fiber>();
    }
    
    /*
     * Clear all assignments (vars), reset jetlang primitives and create a new empty flow.
    */
    public void reset() throws Exception {
        cancelAllFunctions("reset");
        resetJetlangPrimitives();
        clearVertexDelegates();
        clearVars();
        clearBlocks();
        clearFlow();
        clearFunRefs();
    }
    
    private ExecutorService newExecutorService() {
        //return Executors.newFixedThreadPool(8);
        return Executors.newCachedThreadPool();
    }
    
    /*
     * mecha.monitoring.MechaMonitor helpers.
    */
    public int getNumVars() {
        return vars.keySet().size();
    }
    
    public int getNumFuns() {
        return funRefs.keySet().size();
    }
    
    public int getNumBlocks() {
        return blocks.keySet().size();
    }
    
    public int getNumFibers() {
        return fibers.size();
    }
    
    public int getNumMemoryChannels() {
        return memoryChannelMap.keySet().size();
    }
    
    public ExecutorService getFunctionExecutor() {
        return functionExecutor;
    }
    
}
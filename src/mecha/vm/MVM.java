package mecha.vm;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.io.*;
import java.net.*;

import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.Version;

import org.jetlang.channels.*;
import org.jetlang.core.*;
import org.jetlang.fibers.*;

import org.apache.velocity.app.Velocity;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.MethodInvocationException;

import mecha.Mecha;
import mecha.db.*;
import mecha.util.*;
import mecha.client.*;
import mecha.client.net.*;
import mecha.server.*;
import mecha.vm.parser.*;
import mecha.vm.flows.*;
import mecha.json.*;

public class MVM {
    final private static Logger log = 
        Logger.getLogger(MVM.class.getName());
    
    final private static String SYSTEM_NAMESPACE = "$";
    final private static String NS_SEP = ".";
    
    /*
     * Macro pre- and post- expansions.
    */
    final private String[] preMacroDef = {
        "#if ($_is_macro)",
        "${sink} = (macro-sink)",
        "#end",
        "#if (!$_is_macro && !$args.sink)",
        "${sink} = (client-sink)",
        "#end",
        "#if (!$sink) #set ($sink = $args.sink) #end",
        "#if (!$root) #set ($root = $guid) #end"
    };
    final private String[] postMacroDef = {
        "#if (!$_is_macro) ${root} ! (start) #end"
    };
    
    /*
     * Maps a "global block name" to a "block" (list of strings)
    */
    final private ConcurrentHashMap<String, List<String>> globalBlocks;
        
    /*
     * Namespaced verb to RegisteredFunction (which describes
     *  the owning module's class name, etc.)
     *
     * e.g., "riak.get" -> RegisteredFunction
    */
    final private ConcurrentHashMap<String, RegisteredFunction>
        verbMap;
        
    /*
     * Maps the full class name of a module to the initialized
     *  instance of that module.
     *
     * e.g., "mecha.vm.bifs.RiakClientModule" -> RiakClientModule instance.
    */
    final private ConcurrentHashMap<String, MVMModule>
        moduleMap;
    
    public MVM() throws Exception {
        verbMap = new ConcurrentHashMap<String, RegisteredFunction>();
        moduleMap = new ConcurrentHashMap<String, MVMModule>();
        globalBlocks = new ConcurrentHashMap<String, List<String>>();
        Velocity.init();
    }
    
    public void bootstrap() throws Exception {
        /*
         * Connect as a client and execute bootstrap.mvm
         *  functions via "$exec ./mvm/bootstrap.mvm".
        */
        final List<String> bootstrapCommands = 
            TextFile.getLines("./mvm/bootstrap.mvm");
        final String host = 
            Mecha.getConfig()
                 .getJSONObject("riak-config")
                 .getString("pb-ip");
        final String password = Mecha.getConfig().getString("password");
        final int port = Mecha.getConfig().getInt("client-port");
        final AtomicBoolean ready =
            new AtomicBoolean(false);
        final AtomicBoolean bootstrapped =
            new AtomicBoolean(false);
        log.info("bootstrapping...");
        Thread.sleep(1000);
        final TextClient textClient = new TextClient(host, port, password, 
            new MechaClientHandler() {
                public void onMessage(String message) {
                    //log.info("bootstrap: message: " + message);
                }
                
                public void onOpen() {
                    log.info("bootstrap client connected");
                    ready.set(true);
                }
                
                public void onClose() {
                    log.info("bootstrap client disconnected.");
                    log.info("system ready.");
                    bootstrapped.set(true);
                }
                
                public void onError() {
                    log.info("error bootstrapping!");
                }
            });
        final Thread bootstrapThread = new Thread(new Runnable() {
            public void run() {
                try {
                    while(!ready.get()) { Thread.sleep(5); }
                    log.info("executing bootstrap commands...");
                    for(String line : bootstrapCommands) {
                        textClient.send(line);
                    }
                    textClient.send("$bye");
                    log.info("waiting for bootstrap disconnection...");
                    while(!bootstrapped.get()) { Thread.sleep(5); }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        bootstrapThread.start();
    }
    
    public void exec(MVMContext ctx, String filename) throws Exception {
        exec(ctx, new File(filename));
    }
        
    private void exec(MVMContext ctx, File file) throws Exception {
        log.info("exec (" + file + ") context: " + ctx);
        BufferedReader input =  new BufferedReader(new FileReader(file));
        try {
            String line = null;
            while ((line = input.readLine()) != null) {
                execute(null, line);
            }
        } finally {
            input.close();
        }
    }
    
    /*
     * Entry point for all MVM commands.
     *
     * Returns null on success; any other value is 
     *  an error.
     *
     * TODO: Change interface; throw exception?
     *       Return machine-readable JSONObject
     *        representation of error?
     *
    */
    public String execute(MVMContext ctx, String cmd) {
        try {
        
            /*
             * filter blank lines & comments
            */
            cmd = cmd.trim();
            
            if (cmd.equals("")) {
                return null;
            }
            if (cmd.startsWith("//")) {
                //log.info(cmd);
                return null;
            }
            
            /*
             * parse
            */
            MVMParser mvmParser = new MVMParser();
            JSONObject ast = 
                mvmParser.parse(cmd);
                
            /*
             * To make the AST "reversible" for RPC-style
             *  mechanisms (e.g., (warp do:(...))
            */
            ast.put("$_", cmd);
            //log.info("ast = " + ast.toString(4));
            
            String verb = null;
            String operator = null;
            
            /*
             * process "native" operations:
             * 
             * register <namespace> <module-class> <verbs>
             *  e.g., register namespace:$ module-class:mecha.vm.bifs.RiakClientModule verbs:(get:Get put:Put delete:Delete)
             *
             * "=" assignment
             *  e.g., a = (q bucket:users filter:"name_s:j*")
             *
             * "->" wiring
             *  e.g., c <- a b
             *
             * "!" control messaging
             *  e.g., c ! (start)
             *
            */
            
            /*
             * Native multi-part verb + operator + [args .. ] operations.
             * 
             * if "$" field is a list, check for "=", "<-", "->", "!" in the 1st position
            */
            
            if (isList(ast, "$")) {
                
                /*
                 * 1st position operator-driven natives
                 *  e.g., !, ->, <-
                */
                operator = this.<String>getNth(ast, "$", 1);
                //log.info("verb: " + verb + ", operator: " + operator);
                
                /*
                 * A -> B 
                 *  "a flows into b"
                */
                if (operator.equals("->")) {
                    String from = this.<String>getNth(ast, "$", 0);
                    String to   = this.<String>getNth(ast, "$", 2);
                    nativeFlowAddEdge(ctx, from, to);
                }
                                
                /*
                 * a ! (start) 
                 *  "send a the control message (start)"
                */
                else if (operator.equals("!")) {
                    String dest = this.<String>getNth(ast, "$", 0);
                    nativeControlMessage(ctx, dest, this.<JSONObject>getNth(ast, "$args", 0));
                }
                
                /*
                 * a = (query bucket:users filter:"name_s:j*")
                 *  "assign a the expression (query ... )"
                */
                else if (operator.equals("=")) {
                    String var = this.<String>getNth(ast, "$", 0);
                    nativeAssignment(ctx, var, this.<JSONObject>getNth(ast, "$args", 0));
                }
                
                /*
                 * Unknown multi-part verb pattern
                */
                else {
                    throw new Exception("Unknown multi-part verb pattern");
                }
            }
                
            /*
             * Single-verb + [args .. ] operations.
            */
            else {
                verb = this.<String>get(ast, "$");
                
                /*
                 * Native built-in verbs
                */
                if (verb.equals("register")) {
                    nativeRegister(ctx, ast);
                } else if (verb.equals("dump-vars")) {
                    nativeDumpVars(ctx);
                } else if (verb.equals("reset")) {
                    nativeReset(ctx);
                
                /*
                 * Macro expansion
                */
                } else if (verb.startsWith("#")) {
                    nativeMacro(ctx, verb, ast);
                    
                /*
                 * "Auto-wired" invocation
                */
                } else {
                    dynamicInvoke(ctx, verb, ast);
                }
            }
            
        /*
         * An error occurred somewhere in the
         *  processing of a command.  Since MVM is
         *  a plugin-driven system, it
         *  could've been thrown in a lot of unknown
         *  places without context.  Report what we
         *  can say definitively in a format that is
         *  generally useful & return.
        */
        } catch (Exception ex) {
            ex.printStackTrace();
            final Writer result = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(result);
            ex.printStackTrace(printWriter);
            try {
                JSONObject errorObj = new JSONObject();
                errorObj.put("cmd", cmd);
                errorObj.put("error", result.toString());
                return errorObj.toString();
            
            /*
             * Highest probability error here is some kind of
             *  JSON encoding snafu, though there are other
             *  obscure possibilities.
            */
            } catch (Exception ex1) {
                ex1.printStackTrace();
                return "ERROR :" + ex1.toString() + " ON :" + cmd;
            }
        }
        return null;
    }
    
    /*
     * For one-off RPC executions of precomputed ASTs.
    */
    public String execute(MVMContext ctx,
                        JSONObject ast) throws Exception {
        dynamicInvoke(ctx, ast.getString("$"), ast);
        return null;
    }
    
    /*
     * Dynamic invoker.  Returns the assigned refId.
    */
    private void dynamicInvoke(MVMContext ctx, 
                                 String verb, 
                                 JSONObject ast) throws Exception {
        String sinkVar = HashUtils.sha1(Mecha.guid(MVM.class));
        String sourceVar = HashUtils.sha1(Mecha.guid(MVM.class));
        String sourceRefId = nativeAssignment(ctx, sourceVar, ast);
        execute(ctx, sinkVar + " = (client-sink)");
        execute(ctx, sourceVar + " -> " + sinkVar);
        execute(ctx, sourceVar + " ! (start)");
    }
    
    /*
     * Module & verb helpers
    */
    
    private MVMFunction newFunctionInstance(MVMContext ctx, 
                                            String namespacedVerb, 
                                            String refId, 
                                            JSONObject config) throws Exception {
        //log.info("newFunctionInstance: " + namespacedVerb);
        if (!verbMap.containsKey(namespacedVerb)) {
            throw new Exception("Unknown namespaced verb: " + namespacedVerb);
        }
        RegisteredFunction regFun = verbMap.get(namespacedVerb);
        MVMModule mod = moduleMap.get(regFun.getModuleClassName());
        return mod.newFunctionInstance(ctx, regFun.getVerbClassName(), refId, config);
    }
    
    /*
     * "built-in" or "native" verb implementations
    */
    
    /*
     * Wire two assigned module:verb instances together as
     *  a producer-consumer relationship via the current Flow
     *  in ctx.
     *
     * Returns a cluster-wide (globally) unique refId.
     *
    */
    public String nativeFlowAddEdge(MVMContext ctx, String from, String to) 
        throws Exception {
        
        //log.info("nativeFlowAddEdge: from: " + from + " to: " + to);
        
        /*
         * Resolve any origin vertex delegate values.
        */
        from = ctx.resolveVertexDelegate(from);
        
        String fromRefId = ctx.<String>get(from);
        String toRefId = ctx.<String>get(to);
        
        if (fromRefId.equals(toRefId)) {
            throw new Exception("Cannot create a cycle in the data flow graph! " + 
                from + " -> " + to);
        }
            
        String edgeRefId = Mecha.guid(Edge.class);
        JSONObject edgeData = newBaseFlowDataObject(ctx);
        edgeData.put(Flow.REF_ID, edgeRefId);
        edgeData.put("source-vertex", from);
        edgeData.put("target-vertex", to);
        ctx.getFlow().addEdge(edgeRefId,
                              edgeData,
                              fromRefId,
                              toRefId,
                              Flow.FLOW_EDGE_REL,
                              0.0);
        MVMFunction sourceFunction = ctx.getFunRef(fromRefId);
        MVMFunction targetFunction = ctx.getFunRef(toRefId);
        sourceFunction.addOutgoingChannel(Mecha.getChannels().getOrCreateChannel(toRefId));
        targetFunction.addIncomingChannel(Mecha.getChannels().getOrCreateChannel(fromRefId));
        return edgeRefId;
    }
    
    /*
     * Send an arbitrary control message to an assigned var (pointing
     *  to an instance of a module:verb).
     *
     * channel: channel name (control channel name will be automatically
     *  derived).
    */
    public void nativeControlMessage(MVMContext ctx, String channel, JSONObject msg) throws Exception {
        String resolvedChannel = ctx.resolveAssignmentToRefId(channel);
        String controlChannelName = MVMFunction.deriveControlChannelName(resolvedChannel);
        try {
            Mecha.getChannels().getChannel(controlChannelName).send(msg);
        } catch (Exception ex) {
            log.info("Exception on control message: " + msg.toString());
        }
    }
    
    /*
     * Send an arbitrary data message to an assigned var (pointing
     *  to an instance of a module:verb).
     *
     * This is currently only used by the Server "shortcut" operation $data,
     *  which in turn is used only by a WarpDelegate on behalf of a Warp
     *  MVMFunction instance.
    */
    public void nativeDataMessage(MVMContext ctx, String channel, JSONObject msg) throws Exception {
        String dataChannelName = ctx.resolveAssignmentToRefId(channel);
        Mecha.getChannels().getChannel(dataChannelName).send(msg);
    }
    
    /*
     * Perform "a = (expr ...)" assignment to ctx.
    */
    public String nativeAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
        
        /*
         * Process "in-line" (#macro ...) expansions.
        */
        
        String verb = ast.getString("$");
        
        if (verb.startsWith("#")) {
            String macroName = verb.substring(1);
            List<String> blockDef = resolveBlock(ctx, macroName);
            StringBuffer blockStrBuf = new StringBuffer();
            /*
             * Pre-macro wrapper code.
            */
            for(String s : preMacroDef) {
                blockStrBuf.append(s);
                blockStrBuf.append("\n");
            }
            /*
             * Macro body.
            */
            for(String s : blockDef) {
                blockStrBuf.append(s);
                blockStrBuf.append("\n");
            }
            /*
             * Post-macro wrapper code.
            */
            for(String s : postMacroDef) {
                blockStrBuf.append(s);
                blockStrBuf.append("\n");
            }
            String blockStr = blockStrBuf.toString();
            
            String sinkDelegateVar = Mecha.guid(Velocity.class) + "-macro-delegate";
            /*
             * The sink delegate var is actually registered AFTER
             *  the macro is expanded; otherwise it will inevitably
             *  create a cycle in the data flow graph.
            */
            
            VelocityContext context = new VelocityContext();
            context.put("ctx", ctx);
            context.put("args", ast);
            context.put("_is_macro", "true");
            context.put("root", var);
            context.put("sink", sinkDelegateVar);
            context.put("guid", Mecha.guid(Velocity.class));
            
            StringWriter w = new StringWriter();
            Velocity.evaluate(context, w, "#" + verb, blockStr);
            
            String[] renderedMacro = w.toString().split("\n");
            for(String line : renderedMacro) {
                //log.info("n\nExecute: " + line + "\n\n");
                execute(ctx, line);
            }
            
            ctx.setVertexDelegate(var, sinkDelegateVar);
            /*
             * By the time we reach this line, the recursive calls
             *  have defined the passed variable, so we will resolve
             *  the variable to the vertexRefId assigned to it and
             *  return it (since it is the functional source of the
             *  [potential] chain of vertices.
            */
            return ctx.<String>get(var);
        }

        
        /*
         * Standard assignemnt.
        */
        
        String vertexRefId = Mecha.guid(Vertex.class);
        JSONObject vertexData = newBaseFlowDataObject(ctx);
        vertexData.put(Flow.REF_ID, vertexRefId);
        vertexData.put(Flow.CTX_VAR, var);
        vertexData.put(Flow.EXPR, ast);
        ctx.getFlow().addVertex(vertexRefId, vertexData);
        
        /*
         * Map var assignment to underling vertex refId.
        */
        ctx.put(var, vertexRefId);
        
        final String namespacedVerb = this.<String>get(ast, "$");
        MVMFunction inst = 
            newFunctionInstance(ctx, 
                                namespacedVerb, 
                                vertexRefId, 
                                ast);
        ctx.startFunctionTask(vertexRefId, inst);
        ctx.putFunRef(vertexRefId, inst);
        
        /*
         * Trigger MVMFunction "onPostAssignment" event
         *  generally used for self-rewriting functions.
        */
        inst.postAssignment(ctx, var, ast);
        
        /*
         * TODO: create MVMFunction instance,
         *       create jetlang Fiber, MessageChannel
         *       & assign weakly to MVMFunction instance.
        */
        return vertexRefId;
    }
    
    /*
     * Flow helpers
    */
    
    /*
     * Create a template data object for use with creation of Edge and
     *  Vertex Flow instances.
    */
    private JSONObject newBaseFlowDataObject(MVMContext ctx) throws Exception {
        JSONObject data = new JSONObject();
        data.put(Flow.CLIENT_ID, ctx.getClientId());
        data.put(Flow.CONTEXT_REF_ID, ctx.getRefId());
        return data;
    }
    
    /* 
     * Register a class and its inner classes as a module and verbs.
     *
     * AST reference:
     * INFO: ast = {
     *   "$" : "register",
     *   "namespace" : "$",
     *   "module-class" : "mecha.vm.bifs.RiakClientModule",
     *   "verbs" : {
     *     "get" : "Get",
     *     "put" : "Put",
     *     "delete" : "Delete"
     *   }
     * }
     *
    */
    private void nativeRegister(MVMContext ctx, JSONObject ast) throws Exception {
        //log.info("nativeRegister: ast: " + ast);
        
        MVMModule moduleInstance;
        String moduleClassName = ast.getString("module-class");
        String namespace = ast.getString("namespace");
        if (namespace.equals(SYSTEM_NAMESPACE)) {
            namespace = "";
        }

        if (!moduleMap.containsKey(moduleClassName)) {
            log.info("newModuleInstance(" + moduleClassName + ")");
            moduleInstance = MVMModule.newModuleInstance(moduleClassName);
            log.info(moduleClassName + ": moduleLoad()");
            moduleInstance.moduleLoad();
            moduleMap.put(moduleClassName, moduleInstance);
        } else {
            log.info(moduleClassName + ": using existing module instance.");
            moduleInstance = moduleMap.get(moduleClassName);
        }
        
        JSONObject verbs = ast.getJSONObject("verbs");
        for(String verb : verbs.getKeys()) {
            String verbClassName = verbs.getString(verb);
            
            String namespacedVerb;
            if (namespace.equals("")) {
                namespacedVerb = verb;
            } else {
                namespacedVerb = namespace + NS_SEP + verb;
            }
            if (!verbMap.containsKey(namespacedVerb)) {
                log.info("register: " + moduleClassName + ": " + 
                    namespacedVerb + " -> " + verbClassName);
                RegisteredFunction verbFun = 
                    new RegisteredFunction(namespacedVerb,
                                           verbClassName,
                                           moduleClassName);
                verbMap.put(namespacedVerb, verbFun);
            } else {
                log.info("ALREADY REGISTERED: " + moduleClassName + ": " + 
                    namespacedVerb + " -> " + 
                    (verbMap.get(namespacedVerb)).getVerbClassName());
            }
        }
    }
    
    private void nativeMacro(MVMContext ctx, String verb, JSONObject ast)
        throws Exception {
        
        String macroName = verb.substring(1);
        List<String> blockDef = resolveBlock(ctx, macroName);
        StringBuffer blockStrBuf = new StringBuffer();
        /*
         * Pre-macro wrapper code.
        */
        for(String s : preMacroDef) {
            blockStrBuf.append(s);
            blockStrBuf.append("\n");
        }
        /*
         * Macro body.
        */
        for(String s : blockDef) {
            blockStrBuf.append(s);
            blockStrBuf.append("\n");
        }
        /*
         * Post-macro wrapper code.
        */
        for(String s : postMacroDef) {
            blockStrBuf.append(s);
            blockStrBuf.append("\n");
        }
        String blockStr = blockStrBuf.toString();
        
        VelocityContext context = new VelocityContext();
        context.put("ctx", ctx);
        context.put("args", ast);
        context.put("guid", Mecha.guid(Velocity.class));
        
        StringWriter w = new StringWriter();
        Velocity.evaluate(context, w, "#" + verb, blockStr);
        
        String[] renderedMacro = w.toString().split("\n");
        for(String line : renderedMacro) {
            execute(ctx, line);
        }
    }
    
    /*
     * Dump context vars.
    */
    private void nativeDumpVars(MVMContext ctx) throws Exception {
        //log.info("nativeVars");
        
        /*
         * Function assignments.
        */
        JSONObject vars = new JSONObject();
        for(String k : ctx.getVars().keySet()) {
            vars.put(k, ctx.get(k));
        }
        
        /*
         * Memory Channels
        */
        JSONObject memoryChannels = new JSONObject();
        for(String k : ctx.getMemoryChannelMap().keySet()) {
            memoryChannels.put(k, ctx.getMemoryChannelMap().get(k).toString());
        }
        
        /*
         * Blocks.
        */
        JSONObject blocks = new JSONObject();
        for(String k : ctx.getBlockMap().keySet()) {
            blocks.put(k, ctx.getBlock(k));
        }
        
        JSONObject glBlocks = new JSONObject();
        for(String k : getGlobalBlockMap().keySet()) {
            glBlocks.put(k, getGlobalBlock(k));
        }
        
        /*
         * Flow.
        */
        JSONObject flow = new JSONObject();
        JSONArray flowVertices = new JSONArray();
        JSONArray flowEdges = new JSONArray();
        for(Vertex v : ctx.getFlow().getVertices()) {
            flowVertices.put(v);
        }
        for(Edge<Vertex> e : ctx.getFlow().getEdges()) {
            JSONObject edge = new JSONObject();
            edge.put(Flow.REF_ID, e.getRefId());
            edge.put("source-refid", e.getSource().getRefId());
            edge.put("target-refid", e.getTarget().getRefId());
            edge.put(Flow.REL, e.getRel());
            edge.put(Flow.EXPR, e.getExpr());
            flowEdges.put(edge);
        }
        flow.put("vertices", flowVertices);
        flow.put("edges", flowEdges);
        
        JSONObject result = new JSONObject();
        result.put("assignments", vars);
        result.put("memory-channels", memoryChannels);
        result.put("local-blocks", blocks);
        result.put("global-blocks", glBlocks);
        result.put("flow", flow);
        
        ctx.send(result);
        
    }
    
    /*
     * Clear all assignments & create a new, empty flow.
    */
    private void nativeReset(MVMContext ctx) throws Exception {
        ctx.reset();
    }
    
    /*
     * helpers
    */
    
    private boolean isList(JSONObject obj, String field) throws Exception {
        if (obj.get(field) instanceof ArrayList)
            return true;
        return false;
    }
    
    private <T> T getNth(JSONObject obj, String field, int n) throws Exception {
        return (T) obj.getJSONArray(field).get(n);
    }

    private <T> T get(JSONObject obj, String field) throws Exception {
        return (T) obj.get(field);
    }
    
    /*
     * global blocks, helpers
    */
    
    public List<String> resolveBlock(MVMContext ctx, String blockName) {
        List<String> block = ctx.getBlock(blockName);
        if (block == null) {
            block = getGlobalBlock(blockName);
        }
        return block;
    }
    
    public void setGlobalBlock(String blockName, List<String> block) {
        globalBlocks.put(blockName, block);
    }
    
    public void removeGlobalBlock(String blockName) {
        globalBlocks.remove(blockName);
    }
    
    public List<String> getGlobalBlock(String blockName) {
        return globalBlocks.get(blockName);
    }
    
    public ConcurrentHashMap<String, List<String>> getGlobalBlockMap() {
        return globalBlocks;
    }
}
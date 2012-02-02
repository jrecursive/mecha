package mecha.vm;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;

import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.Version;

import org.jetlang.channels.*;
import org.jetlang.core.*;
import org.jetlang.fibers.*;

import mecha.json.*;

import mecha.Mecha;
import mecha.db.*;
import mecha.util.*;
import mecha.server.*;
import mecha.vm.parser.*;
import mecha.vm.flows.*;

public class MVM {
    final private static Logger log = 
        Logger.getLogger(MVM.class.getName());
    
    final private static String SYSTEM_NAMESPACE = "$";
    final private static String NS_SEP = ".";
        
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
    
        bootstrap();
    }
    
    private void bootstrap() throws Exception {
        exec(null, "./mvm/bootstrap.mvm");
    }
    
    private void exec(MVMContext ctx, String filename) throws Exception {
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
            if (cmd.startsWith("##")) {
                log.info(cmd);
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
                //log.info("verb: " + verb);
                
                /*
                 * Native built-in verbs
                */
                if (verb.equals("register")) {
                    nativeRegister(ctx, ast);
                } else if (verb.equals("dump-vars")) {
                    nativeDumpVars(ctx);
                } else if (verb.equals("reset")) {
                    nativeReset(ctx);
                }
                
                /*
                 * "Auto-wired" invocation
                */
                else {
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
    public String nativeFlowAddEdge(MVMContext ctx, String from, String to) throws Exception {
        //log.info("nativeFlowAddEdge: from: " + from + " to: " + to);
        
        String fromRefId = ctx.<String>get(from);
        String toRefId = ctx.<String>get(to);
        
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
        //log.info("<" + controlChannelName + "> ! " + msg.toString(2));
        Mecha.getChannels().getChannel(controlChannelName).send(msg);
        //log.info("nativeControlMessage: channel: " + controlChannelName + " msg: " + msg);
    }
    
    /*
     * Perform "a = (expr ...)" assignment to ctx.
    */
    public String nativeAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
        //log.info("nativeAssignment: var: " + var + " ast: " + ast);
        
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
    
    /*
     * Dump context vars.
    */
    private void nativeDumpVars(MVMContext ctx) throws Exception {
        //log.info("nativeVars");
        
        JSONObject vars = new JSONObject();
        for(String k : ctx.getVars().keySet()) {
            vars.put(k, ctx.get(k));
        }
        for(String k : ctx.getMemoryChannelMap().keySet()) {
            vars.put("MemoryChannel-" + k, ctx.getMemoryChannelMap().get(k).toString());
        }
        ctx.send(vars);
        
    }
    
    /*
     * Clear all assignments & create a new, empty flow.
    */
    private void nativeReset(MVMContext ctx) throws Exception {
        //log.info("nativeReset");
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
     * flow
    */
    
    
}
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

public class MVM {
    final private static Logger log = 
        Logger.getLogger(MVM.class.getName());
    
    final private static String SYSTEM_NAMESPACE = "$";
    final private static String NS_SEP = ".";
    
    /*
     * Jetlang
    */    
    final private ExecutorService functionExecutor;
    final private PoolFiberFactory fiberFactory;
    
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
    
        functionExecutor = Executors.newCachedThreadPool();
        fiberFactory = new PoolFiberFactory(functionExecutor);
        
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
            log.info("ast = " + ast.toString(4));
            
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
                log.info("verb: " + verb + ", operator: " + operator);
                
                /*
                 * A -> B 
                 *  "a flows into b"
                */
                if (operator.equals("->")) {
                    String from = this.<String>getNth(ast, "$", 0);
                    String to   = this.<String>getNth(ast, "$", 2);
                    nativeFlow(ctx, from, to);
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
                log.info("verb: " + verb);
                
                /*
                 * Native built-in verbs
                */
                if (verb.equals("register")) {
                    nativeRegister(ctx, ast);
                } else if (verb.equals("dump-vars")) {
                    nativeVars(ctx);
                } 
                
                /*
                 * Dynamic invocation
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
     * Dynamic invoker
    */
    private void dynamicInvoke(MVMContext ctx, 
                               String verb, 
                               JSONObject ast) throws Exception {
        log.info("dynamicInvoke: ctx: " + ctx + " verb: " + verb + " ast: " + ast.toString(2));
        MVMFunction fun = newFunctionInstance(ctx, verb, ast);
    }
    
    /*
     * Module & verb helpers
    */
    
    private MVMFunction newFunctionInstance(MVMContext ctx, 
                                            String namespacedVerb, 
                                            JSONObject config) throws Exception {
        log.info("newFunctionInstance: " + namespacedVerb);
        if (!verbMap.containsKey(namespacedVerb)) {
            log.info("Unknown namespaced verb: " + namespacedVerb);
            return null;
        }
        RegisteredFunction regFun = verbMap.get(namespacedVerb);
        MVMModule mod = moduleMap.get(regFun.getModuleClassName());
        return mod.newFunctionInstance(ctx, regFun.getVerbClassName(), config);
    }
    
    /*
     * "built-in" or "native" verb implementations
    */
    
    /*
     * Wire two assigned module:verb instances together as
     *  a producer-consumer relationship via the current Flow
     *  in ctx.
    */
    private void nativeFlow(MVMContext ctx, String from, String to) throws Exception {
        log.info("nativeFlow: from: " + from + " to: " + to);
    }
    
    /*
     * Send an arbitrary control message to an assigned var (pointing
     *  to an instance of a module:verb).
    */
    private void nativeControlMessage(MVMContext ctx, String dest, JSONObject msg) throws Exception {
        log.info("nativeControlMessage: dest: " + dest + " msg: " + msg);
    }
    
    /*
     * Perform "a = (expr ...)" assignment to ctx.
    */
    private void nativeAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
        log.info("nativeAssignment: var: " + var + " ast: " + ast);
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
        log.info("nativeRegister: ast: " + ast);
        
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
    private void nativeVars (MVMContext ctx) throws Exception {
        log.info("nativeVars");
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
    
    private String guid() throws Exception {
        String nodeId = Mecha.getConfig().getString("riak-nodename");
        return nodeId + "/" + UUID.randomUUID();
    }
}
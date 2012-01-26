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
    
    public MVM() {
        verbMap = new ConcurrentHashMap<String, RegisteredFunction>();
        moduleMap = new ConcurrentHashMap<String, MVMModule>();
    
        functionExecutor = Executors.newCachedThreadPool();
        fiberFactory = new PoolFiberFactory(functionExecutor);
    }
    
    public String execute(MVMContext ctx, String cmd) {
        try {
        
            /*
             * setup
            */
            
            MVMParser mvmParser = new MVMParser();
            Client client = ctx.getClient();
            
            /*
             * parse
            */
            
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
             * "<-" & "->" wiring
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
                 *  "send a the control message (start)
                */
                if (operator.equals("!")) {
                    String dest = this.<String>getNth(ast, "$", 0);
                    nativeControlMessage(ctx, dest, this.<JSONObject>getNth(ast, "$args", 0));
                }
                
                /*
                 * a = (query bucket:users filter:"name_s:j*")
                 *  "assign a the expression (query ... )"
                */
                if (operator.equals("=")) {
                    String var = this.<String>getNth(ast, "$", 0);
                    nativeAssignment(ctx, var, this.<JSONObject>getNth(ast, "$args", 0));
                }
                
            /*
             * Native single-verb + [args .. ] operations.
            */
            } else {
                verb = this.<String>get(ast, "$");
                log.info("verb: " + verb);
                
                if (verb.equals("register")) {
                    nativeRegister(ctx, ast);
                }
                
                if (verb.equals("dump-vars")) {
                    nativeVars(ctx);
                }
                
            }
            
            /*
             * optimize
            */
            
            /*
             * execute
            */
            
            /*
             * report result
            */
        
        /*
         * An error occurred somewhere in the
         *  processing of a command.  Since MVM is
         *  based on a plugin-driven system, it
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
        
        return "x"; // tmp
    }
    
    /*
     * native verb implementations
    */
    
    private void nativeFlow(MVMContext ctx, String from, String to) throws Exception {
        log.info("nativeFlow: from: " + from + " to: " + to);
    }
    
    private void nativeControlMessage(MVMContext ctx, String dest, JSONObject msg) throws Exception {
        log.info("nativeControlMessage: dest: " + dest + " msg: " + msg);
    }
    
    private void nativeAssignment(MVMContext ctx, String var, JSONObject ast) throws Exception {
        log.info("nativeAssignment: var: " + var + " ast: " + ast);
    }
    
    private void nativeRegister(MVMContext ctx, JSONObject ast) throws Exception {
        log.info("nativeRegister: ast: " + ast);
    }
    
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
}
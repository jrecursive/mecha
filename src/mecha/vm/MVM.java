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

import org.json.*;

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

}
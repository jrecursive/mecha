package mecha.vm;

import java.util.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.Version;

import org.json.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.jgrapht.ext.*;

import mecha.Mecha;
import mecha.db.*;
import mecha.util.*;
import mecha.server.*;
import mecha.vm.parser.*;

public class MVM {
    final private static Logger log = 
        Logger.getLogger(MVM.class.getName());
    
    public String execute(MVMContext ctx, String cmd) {
        try {
        
            /*
             * setup
            */
            
            CommandParser commandParser = new CommandParser();
            Client client = ctx.getClient();
            
            // TODO: debug switch
            commandParser.SHOW_GRAPH = true;
            
            /*
             * parse
            */
            
            ListenableDirectedGraph<QueryNode, QueryEdge> ast = 
                commandParser.parse("$", cmd);
            
            log.info("ast = " + ast.toString());
            
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
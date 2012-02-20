package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;

public class SystemModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(SystemModule.class.getName());
    
    public SystemModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }
    
    public class Invoke extends MVMFunction {
        final private List<String> scriptBlock;
        final private String scriptBlockName;
        final private ScriptEngine scriptEngine;
        
        public Invoke(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            scriptBlockName = config.get("script");
            scriptBlock = Mecha.getMVM().resolveBlock(getContext(), scriptBlockName);
            
            scriptEngine = new ScriptEngine("js");
            scriptEngine.bind("$log", log);
            /*
                (JSONObject) scriptEngine.invoke("$preprocess", obj);
                scriptEngine.eval(codeBlock.toString());
            */
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
            
            /*
             * .. xxx message passing test code
            */
            JSONObject newMsg = new JSONObject();
            newMsg.put("welcome", "to the new database");
            broadcastDataMessage(newMsg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
        }
    }
    
    public class Put extends MVMFunction {
        public Put(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
        }

    }
    
    public class Delete extends MVMFunction {
        public Delete(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
        }

    }
}
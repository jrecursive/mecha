package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import mecha.json.*;

import mecha.vm.*;

public class RiakClientModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(RiakClientModule.class.getName());
    
    public RiakClientModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");   
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
    }
    
    public class Get extends MVMFunction {
        public Get(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
    
    public class Put extends MVMFunction {
        public Put(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
    
    public class Delete extends MVMFunction {
        public Delete(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
}
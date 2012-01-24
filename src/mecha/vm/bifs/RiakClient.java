package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import org.json.*;

import mecha.vm.MVMModule;
import mecha.vm.MVMFunction;

public class RiakClient extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(RiakClient.class.getName());
    
    final private String test = "hi";

    public RiakClient() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");   
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
    }
    
    public class Get extends MVMFunction {
        public Get(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            log.info("test = " + test);
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
    
    public class Put extends MVMFunction {
        public Put(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            log.info("test = " + test);
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
    
    public class Delete extends MVMFunction {
        public Delete(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            log.info("test = " + test);
        }

        public void data(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
        }
    }
    


}
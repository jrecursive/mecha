package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import org.json.*;

import mecha.vm.MVMModule;
import mecha.vm.MVMFunction;

public class ChannelModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ChannelModule.class.getName());

    public ChannelModule() throws Exception {
        super();
    }
        
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleunLoad()");
    }
    
    public class Subscribe extends MVMFunction {
        JSONObject state;
    
        public Subscribe(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
            
        }
        
        public void control(JSONObject msg) throws Exception {}

        public void data(JSONObject msg) throws Exception {}
    }

    public class Unsubscribe extends MVMFunction {
        public Unsubscribe(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void data(JSONObject msg) throws Exception {}
    }

    public class Publish extends MVMFunction {
        public Publish(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
        
        public void control(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
        }

        public void data(JSONObject msg) throws Exception {}
    }

    
}
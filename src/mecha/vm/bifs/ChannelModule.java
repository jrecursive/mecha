package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.json.*;

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
        public Subscribe(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
    }

    public class Unsubscribe extends MVMFunction {
        public Unsubscribe(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
    }

    public class Publish extends MVMFunction {
        public Publish(JSONObject config) throws Exception {
            super(config);
            log.info("constructor: " + config.toString(2));
        }
    }
}
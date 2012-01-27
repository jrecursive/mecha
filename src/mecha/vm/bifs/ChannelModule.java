package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.json.*;

import mecha.vm.*;

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
        public Subscribe(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
    }

    public class Unsubscribe extends MVMFunction {
        public Unsubscribe(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
    }

    public class Publish extends MVMFunction {
        public Publish(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            log.info("constructor: " + config.toString(2));
        }
    }
}
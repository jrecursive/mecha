package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.json.*;

import mecha.vm.*;

public class ClientModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(ClientModule.class.getName());

    public ClientModule() throws Exception {
        super();
    }

    public void moduleLoad() throws Exception { }
    
    public void moduleUnload() throws Exception { }
    
    /*
     * ClientSink is a generic "sink" function that forwards
     *  any control or data inputs to the calling context's
     *  endpoint (which is, for now, always a websocket connection).
    */
    public class ClientSink extends MVMFunction {
    
        public ClientSink(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
    
        public void onControlMessage(JSONObject msg) throws Exception {
            msg.put("$type", "control");
            getContext().send(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            msg.put("$type", "data");
            getContext().send(msg);
        }
    }
}
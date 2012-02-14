package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.lang.ref.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.text.Collator;

import mecha.Mecha;
import mecha.jinterface.*;
import mecha.util.HashUtils;
import mecha.json.*;
import mecha.vm.*;
import mecha.client.*;
import mecha.client.net.*;

public class RelationalModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(RelationalModule.class.getName());
    
    public RelationalModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }

    /*
     * Streaming sort-merge equijoin, a la 
     *  http://en.wikipedia.org/wiki/Sort-merge_join
    */    
    public class SortMergeEquiJoin extends MVMFunction {
        public SortMergeEquiJoin(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            
        }
    }
    
}
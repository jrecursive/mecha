/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
    
    /*
     * Commit performs a commit operation on
     *  both the local Solr "index" core and
     *  the object store.
    */
    public class Commit extends MVMFunction {
        Map<String, Integer> facetMap;
        
        public Commit(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            long t_st = System.currentTimeMillis();
            //Mecha.getSolrManager().getIndexServer().commit(true,true);
            try {
                Mecha.getMDB().commit();
            } catch (Exception ex) {
                ex.printStackTrace();
                log.info("MDB commit failed! Rolling back index core commit!");
                // TODO: solr rollback
                throw ex;
            }
            long t_elapsed = System.currentTimeMillis() - t_st;
            JSONObject response = new JSONObject();
            response.put("host", Mecha.getHost());
            response.put("elapsed", t_elapsed);
            broadcastDataMessage(response);
            broadcastDone();
        }
    }
    
    public class Repeater extends MVMFunction {
        final private boolean splitData;
        final private boolean splitControl;
        final private boolean splitStart;
        final private boolean splitDone;
        final private List<String> destinations;
        
        public Repeater(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            if (config.has("data") &&
                config.<String>get("data").equals("true")) {
                splitData = true;
            } else {
                splitData = false;
            }
            if (config.has("control") &&
                config.<String>get("control").equals("true")) {
                splitControl = true;
            } else {
                splitControl = false;
            }
            if (config.has("start") &&
                config.<String>get("start").equals("true")) {
                splitStart = true;
            } else {
                splitStart = false;
            }
            if (config.has("done") &&
                config.<String>get("control").equals("true")) {
                splitDone = true;
            } else {
                splitDone = false;
            }
            destinations = config.<List>get("to");
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            if (splitControl) repeatControlMessage(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
            if (splitData) repeatDataMessage(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
            if (splitStart) repeatControlMessage(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
            if (splitDone) repeatControlMessage(msg);
        }
        
        private void repeatControlMessage(JSONObject msg) throws Exception {
            for(String dest : destinations) {
                Mecha.getMVM().nativeControlMessage(getContext(), dest, msg);
            }
        }
        
        private void repeatDataMessage(JSONObject msg) throws Exception {
            for(String dest : destinations) {
                Mecha.getMVM().nativeDataMessage(getContext(), dest, msg);
            }
        }
    }
    
    public class Splitter extends MVMFunction {
        final private boolean splitData;
        final private boolean splitControl;
        
        public Splitter(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            if (config.has("data") &&
                config.<String>get("data").equals("true")) {
                splitData = true;
            } else {
                splitData = false;
            }
            if (config.has("control") &&
                config.<String>get("control").equals("true")) {
                splitControl = true;
            } else {
                splitControl = false;
            }            
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            if (splitControl) broadcastControlMessage(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
            if (splitData) broadcastDataMessage(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
            if (splitControl) broadcastControlMessage(msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
            if (splitControl) broadcastControlMessage(msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
            if (splitControl) broadcastControlMessage(msg);
        }
    }
    
    public class Invoke extends MVMFunction {
        final private List<String> scriptBlock;
        final private String scriptBlockName;
        final private ScriptEngine scriptEngine;
        
        public Invoke(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            scriptBlockName = config.get("script");
            scriptBlock = Mecha.getMVM().resolveBlock(getContext(), scriptBlockName);
            
            final JSONObject args;
            if (config.has("args")) {
                args = config.getJSONObject("args");
            } else {
                args = new JSONObject();
            }
            
            scriptEngine = new ScriptEngine("js");
            scriptEngine.bind("$refid", refId);
            scriptEngine.bind("$context", ctx);
            scriptEngine.bind("$args", args);
            scriptEngine.bind("$log", log);
            scriptEngine.bind("$mecha", Mecha.get());
            scriptEngine.bind("$", this);
            
            scriptEngine.eval(render(scriptBlock));
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            log.info("Control message: " + msg.toString(2));
            scriptEngine.invoke("onControlMessage", msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            log.info("Data message: " + msg.toString(2));
            scriptEngine.invoke("onDataMessage", msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            log.info("onStartEvent: " + msg.toString(2));
            scriptEngine.invoke("onStartEvent", msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            log.info("onCancelEvent: " + msg.toString(2));
            scriptEngine.invoke("onCancelEvent", msg);
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            log.info("onDoneEvent: " + msg.toString(2));
            scriptEngine.invoke("onDoneEvent", msg);
        }
        
        private String render(List<String> lines) throws Exception {
            StringBuffer sb = new StringBuffer();
            for(String line: lines) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString();
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
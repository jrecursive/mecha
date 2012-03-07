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
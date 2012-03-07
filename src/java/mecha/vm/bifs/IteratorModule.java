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

public class IteratorModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(IteratorModule.class.getName());
    
    public IteratorModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }

    /*
     * Streaming sort-merge equijoin, a la 
     *  http://en.wikipedia.org/wiki/Sort-merge_join
     *
     * (sort-merge-equijoin left:(input:a field:f0) right:(input:b field:f1))
     *
    */    
    public class BufferedIterator extends MVMFunction {
        final private LinkedBlockingQueue<JSONObject> buffer;
        final private LinkedBlockingQueue<JSONObject> controlQueue;
        final private Thread controlThread;
        
        public BufferedIterator(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            buffer = new LinkedBlockingQueue<JSONObject>();
            controlQueue = new LinkedBlockingQueue<JSONObject>();
            controlThread = new Thread(new Runnable() {
                public void run() {
                    while(true) {
                        try {
                            JSONObject msg = controlQueue.take();
                            if (msg.has("$")) {
                                String operation = msg.<String>get("$");
                                
                                if (operation.equals("next")) {
                                    JSONObject bufferedData = buffer.take();
                                    broadcastDataMessage(bufferedData);
                                    if (bufferedData.has("$") &&
                                        bufferedData.<String>get("$").equals("done")) {
                                        log.info("<control-thread> done: " + bufferedData.toString());
                                        return;
                                    }
                                } else {
                                    log.info("<controlThread> unknown operation '" + operation + "': " +
                                        msg.toString());
                                }
                            } else {
                                log.info("<controlThread> unknown controlQueue message: " + msg.toString());
                            }
                        } catch (InterruptedException iex) {
                            return;
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.vm.bifs.iterator-module", ex);
                            ex.printStackTrace();
                            log.info("<controlThread> exiting control thread.");
                            return;
                        }
                    }
                }
            });
            controlThread.start();
            
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            //log.info("<control> " + msg.toString());
            controlQueue.offer(msg);
        }

        public void onDataMessage(JSONObject msg) throws Exception {
            buffer.put(msg);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            //log.info("<start> " + msg.toString());
        }
        
        public void onDoneEvent(JSONObject msg) throws Exception {
            buffer.put(msg);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            //log.info("<cancel> " + msg.toString());
            controlThread.interrupt();
        }
    }
}

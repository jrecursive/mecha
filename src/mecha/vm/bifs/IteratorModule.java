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
        final private AtomicBoolean done;
        private JSONObject upstreamDoneMsg;
        
        public BufferedIterator(String refId, MVMContext ctx, JSONObject config) 
            throws Exception {
            super(refId, ctx, config);
            done = new AtomicBoolean(false);
            done.set(false);
            upstreamDoneMsg = null;
            buffer = new LinkedBlockingQueue<JSONObject>();
            controlQueue = new LinkedBlockingQueue<JSONObject>();
            controlThread = new Thread(new Runnable() {
                public void run() {
                    while(true) {
                        try {
                            JSONObject msg = controlQueue.take();
                            if (done.get()) {
                                log.info(msg.toString());
                            }
                            if (msg.has("$")) {
                                String operation = msg.<String>get("$");
                                
                                if (operation.equals("next")) {
                                    JSONObject bufferedData = buffer.take();
                                    if (bufferedData.has("$") &&
                                        bufferedData.<String>get("$").equals("done")) {
                                        log.info("<control-thread> done: " + bufferedData.toString());
                                    }
                                    broadcastDataMessage(bufferedData);
                                    
                                    //log.info(buffer.size() + " -> " + bufferedData.toString());
                                    
                                } else {
                                    log.info("<controlThread> unknown operation '" + operation + "': " +
                                        msg.toString());
                                }
                            } else {
                                log.info("<controlThread> unknown controlQueue message: " + msg.toString());
                            }
                        } catch (Exception ex) {
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
            log.info("<done> " + msg.toString());
            buffer.put(msg);
            log.info("Buffer queue size: " + buffer.size());
            done.set(true);
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            //log.info("<cancel> " + msg.toString());
            controlThread.interrupt();
        }
    }
}

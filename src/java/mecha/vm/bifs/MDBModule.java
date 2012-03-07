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
import java.util.concurrent.locks.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;
import java.lang.ref.*;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.response.FacetField;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;
import mecha.db.*;

public class MDBModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(MDBModule.class.getName());
    
    public MDBModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
    }
    
    public void moduleUnload() throws Exception {
    }
    
    /*
     * materialize-pbk-stream
    */
    public class MaterializePBKStream extends MVMFunction {
        public MaterializePBKStream(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            final String partition = msg.getString("partition");
            final String bucket = msg.getString("bucket");
            final String key = msg.getString("key");
            byte[] bytes = Mecha.getMDB()
                                .getBucket(partition, bucket)
                                .get(key.getBytes());
            broadcastDataMessage(
                new JSONObject(new String(bytes)));
        }
    }
    
    /*
     * global-drop-bucket
    */
    public class GlobalDropBucket extends MVMFunction {
        final private String bucketName;
        
        public GlobalDropBucket(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            bucketName = config.getString("bucket");
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            long t_st = System.currentTimeMillis();
            int count = Mecha.getMDB().globalDropBucket(bucketName);
            long t_elapsed = System.currentTimeMillis() - t_st;
            JSONObject dataMsg = new JSONObject();
            dataMsg.put("value", "cluster-time-elapsed");
            dataMsg.put("count", t_elapsed);
            broadcastDataMessage(dataMsg);
            
            dataMsg = new JSONObject();
            dataMsg.put("value", "cluster-bucket-instances-dropped");
            dataMsg.put("count", count);
            broadcastDataMessage(dataMsg);
            
            dataMsg = new JSONObject();
            dataMsg.put("value", Mecha.getHost() + "-time-elapsed");
            dataMsg.put("count", t_elapsed);
            broadcastDataMessage(dataMsg);

            dataMsg = new JSONObject();
            dataMsg.put("value", Mecha.getHost() + "-bucket-instances-dropped");
            dataMsg.put("count", count);
            broadcastDataMessage(dataMsg);
            
            broadcastDone();
        }
    }
    
    /*
     * drop-partition-bucket
    */
    public class DropPartitionBucket extends MVMFunction {
        final private String partition;
        final private String bucketName;
        
        public DropPartitionBucket(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            partition = config.getString("partition");
            bucketName = config.getString("bucket");
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            JSONObject dataMsg = new JSONObject();
            long t_st = System.currentTimeMillis();
            Mecha.getMDB().dropBucket(partition, bucketName.getBytes());
            long t_elapsed = System.currentTimeMillis() - t_st;
            dataMsg.put("value", "cluster-time-elapsed");
            dataMsg.put("count", t_elapsed);
            broadcastDataMessage(dataMsg);
            broadcastDone();
        }
    }
    
    /*
     * stream-partition-bucket
    */
    public class StreamPartitionBucket extends MVMFunction {
        final private WeakReference<Bucket> bucket;
        final private String partition;
        final private String bucketName;
        
        public StreamPartitionBucket(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            partition = config.getString("partition");
            bucketName = config.getString("bucket");
            bucket = new WeakReference<Bucket>(Mecha.getMDB().getBucket(partition, bucketName));
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            final AtomicInteger count = new AtomicInteger(0);
            bucket.get().foreach(new MDB.ForEachFunction() {
                public boolean each(byte[] bucket, byte[] key, byte[] value) {
                    try {
                        JSONObject msg = new JSONObject(new String(value));
                        broadcastDataMessage(msg);
                        count.getAndIncrement();
                    } catch (Exception ex) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", ex);
                        ex.printStackTrace();
                    }
                    return true;
                }
            });
            JSONObject doneMsg = new JSONObject();
            doneMsg.put("count", count.intValue());
            broadcastDone(doneMsg);
        }
    }
    
    /*
     * stream-partition-bucket
    */
    public class ComputeSchema extends MVMFunction {
        final private WeakReference<Bucket> bucket;
        final private String partition;
        final private String bucketName;
        final private int maxSamples;
        
        public ComputeSchema(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            partition = config.getString("partition");
            bucketName = config.getString("bucket");
            bucket = new WeakReference<Bucket>(Mecha.getMDB().getBucket(partition, bucketName));
            maxSamples = Integer.parseInt(config.getString("max-samples"));
        }
        
        public void onStartEvent(JSONObject startMsg) throws Exception {
            final AtomicInteger count = new AtomicInteger(0);
            final Map<String, Integer> schemaHistogram = 
                new HashMap<String, Integer>();
            bucket.get().foreach(new MDB.ForEachFunction() {
                public boolean each(byte[] bucket, byte[] key, byte[] value) {
                    try {
                        JSONObject msg = new JSONObject(new String(value));
                        JSONObject obj = new JSONObject(msg.getJSONArray("values")
                                                           .getJSONObject(0)
                                                           .getString("data"));
                        for(String f : JSONObject.getNames(obj)) {
                            if (!schemaHistogram.containsKey(f)) {
                                schemaHistogram.put(f, 1);
                            } else {
                                int c = schemaHistogram.get(f);
                                schemaHistogram.put(f, c+1);
                            }
                        }
                        count.getAndIncrement();
                        if (maxSamples > 0 &&
                            count.get() == maxSamples) return false;
                    } catch (Exception ex) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", ex);
                        ex.printStackTrace();
                    }
                    return true;
                }
            });
            for(String k : schemaHistogram.keySet()) {
                JSONObject msg = new JSONObject();
                msg.put("value", k);
                msg.put("count", schemaHistogram.get(k));
                broadcastDataMessage(msg);                
            }
            
            JSONObject doneMsg = new JSONObject();
            doneMsg.put("count", count.intValue());
            broadcastDone(doneMsg);
        }
    }

    
    /*
     * partition-bucket-iterator
    */
    public class PartitionBucketIterator extends MVMFunction {
        final private WeakReference<Bucket> bucket;
        final private String partition;
        final private String bucketName;
        
        /*
         * Dedicated iterator thread & control channel communication mechanisms.
        */
        final private AtomicBoolean next;
        final private AtomicBoolean stop;
        final private ReentrantLock stateLock;
        final private Thread iteratorThread;
        final private String iterationLabel;
        
        public PartitionBucketIterator(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            partition = config.getString("partition");
            bucketName = config.getString("bucket");
            bucket = new WeakReference<Bucket>(Mecha.getMDB().getBucket(partition, bucketName));
            next = new AtomicBoolean(false);
            stop = new AtomicBoolean(false);
            stateLock = new ReentrantLock();
            
            if (config.has("iterator-name")) {
                iterationLabel = config.getString("iterator-name");
            } else {
                iterationLabel = null;
            }
            
            /*
             * Dedicated bucket iterator thread.
            */
            final Runnable runnableIterator = new Runnable() {
                public void run() {
                    try {
                        final AtomicInteger count = new AtomicInteger(0);
                        final AtomicBoolean earlyExit = new AtomicBoolean(false);
                        final JSONObject doneMsg = new JSONObject();
                        try {
                            bucket.get().foreach(new MDB.ForEachFunction() {
                                public boolean each(byte[] bucket, byte[] key, byte[] value) {
                                    try {
                                        while(!next.get()) {
                                            if (stop.get()) {
                                                earlyExit.set(true);
                                                return false;
                                            }
                                            Thread.sleep(5);
                                        }
                                        stateLock.lock();
                                        try {
                                            JSONObject msg = new JSONObject(new String(value));
                                            if (iterationLabel != null) {
                                                msg.put("$iterator", iterationLabel);
                                            }
                                            broadcastDataMessage(msg);
                                            count.getAndIncrement();
                                            next.set(false);
                                        } finally {
                                            stateLock.unlock();
                                        }
                                    } catch (Exception ex) {
                                        Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", ex);
                                        ex.printStackTrace();
                                        try {
                                            earlyExit.set(true);
                                            doneMsg.put("iterator-exception",
                                                Mecha.exceptionToStringArray(ex));
                                        } catch (Exception _ex) {
                                            Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", _ex);
                                            _ex.printStackTrace();
                                        }
                                        return false;
                                    }
                                    return true;
                                }
                            });
                        } catch (Exception ex) {
                            Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", ex);
                            log.info("iterator thread exception!");
                            ex.printStackTrace();
                            try {
                                doneMsg.put("exception", Mecha.exceptionToStringArray(ex));
                            } catch (Exception _ex) {
                                Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", _ex);
                                _ex.printStackTrace();
                            }
                        }
                        doneMsg.put("stopped", earlyExit.get());
                        doneMsg.put("count", count.intValue());
                        broadcastDone(doneMsg);
                    } catch (Exception ex1) {
                        Mecha.getMonitoring().error("mecha.vm.bifs.mdb-module", ex1);
                        ex1.printStackTrace();
                    }
                }
            };
            iteratorThread = new Thread(runnableIterator);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            iteratorThread.start();
        }
        
        public void onCancelEvent(JSONObject msg) throws Exception {
            stop.set(true);
            if (iteratorThread.isAlive()) {
                iteratorThread.interrupt();
            }
        }
        
        public void onControlMessage(JSONObject msg) throws Exception {
            if (!iteratorThread.isAlive()) {
                log.info("iterator thread has not been started!");
                return;
            }
            
            final String verb = msg.getString("$");
            
            stateLock.lock();
            try {
                /*
                 * "next" - send one record down the pipeline.
                */
                if (verb.equals("next")) {
                    next.set(true);
                    
                /*
                 * "stop" - cancel the iteration.
                */
                } else if (verb.equals("stop")) {
                    stop.set(true);
                    
                /*
                 * unknown
                */
                } else {
                    log.info("Unknown state! msg = " + msg.toString(2));
                }
            } finally {
                stateLock.unlock();
            }
        }
    }
}
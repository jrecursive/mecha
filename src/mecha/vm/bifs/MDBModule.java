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
    
    public class MaterializePBKStream extends MVMFunction {
        public MaterializePBKStream(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onDataMessage(JSONObject msg) throws Exception {
            final String partition = msg.getString("partition");
            final String bucket = msg.getString("bucket");
            final String key = msg.getString("key");
            broadcastDataMessage(
                new JSONObject(new String(
                    Mecha.getMDB()
                         .getBucket(partition, bucket)
                         .get(key.getBytes()))));
        }
    }
    
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
                                        ex.printStackTrace();
                                        try {
                                            earlyExit.set(true);
                                            doneMsg.put("iterator-exception",
                                                Mecha.exceptionToStringArray(ex));
                                        } catch (Exception _ex) { _ex.printStackTrace(); }
                                        return false;
                                    }
                                    return true;
                                }
                            });
                        } catch (Exception ex) {
                            log.info("iterator thread exception!");
                            ex.printStackTrace();
                            try {
                                doneMsg.put("exception", Mecha.exceptionToStringArray(ex));
                            } catch (Exception _ex) { _ex.printStackTrace(); }
                        }
                        doneMsg.put("stopped", earlyExit.get());
                        doneMsg.put("count", count.intValue());
                        broadcastDone(doneMsg);
                    } catch (Exception ex1) {
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
            log.info("control message : " + msg.toString(2));
            
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
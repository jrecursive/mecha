package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
}
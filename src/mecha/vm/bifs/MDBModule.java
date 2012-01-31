package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.response.FacetField;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;

public class MDBModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(MDBModule.class.getName());
    
    public MDBModule() throws Exception {
        super();
    }
    
    public void moduleLoad() throws Exception {
        log.info("moduleLoad()");   
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
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
}
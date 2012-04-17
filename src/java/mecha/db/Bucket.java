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

package mecha.db;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;
import java.text.SimpleDateFormat;

import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;
import mecha.monitoring.*;

public class Bucket {
    final private static Logger log = 
        Logger.getLogger(Bucket.class.getName());
        
    final static private Rates rates = new Rates();
    static {
        Mecha.getMonitoring().addMonitoredRates(rates);
    }
    
    final private static int MURMUR_SEED = 12261976;
    
    final private byte[] bucket;
    final private String bucketStr;
    final private String partition;
    final private String dataDir;
    final private SolrServer solrServer;
    
    final private SimpleDateFormat dateFormat;

    private static String STANDARD_DATE_FORMAT = 
        "yyyy-MM-dd'T'HH:mm:ss";
    
    public Bucket(String partition, 
                  byte[] bucket, 
                  String dataDir) throws Exception {
        this.partition = partition;
        this.bucket = bucket;
        this.bucketStr = (new String(bucket)).trim();
        this.dataDir = dataDir;
        dateFormat = new java.text.SimpleDateFormat(STANDARD_DATE_FORMAT);
        
        // update to use partition-specific core?
        //solrServer = Mecha.getSolrManager().getCore("index").getServer();
        synchronized(Bucket.class) {
            solrServer = Mecha.getSolrManager().getPartitionCore(partition).getServer();
        }
        
        log.info("Bucket: " + partition + ": " + bucketStr + ": " + dataDir);
    }
        
    public void stop() throws Exception {
        log.info("stop: " + bucketStr + ", " + partition + ": " + dataDir);
    }
    
    public byte[] get(byte[] key) throws Exception {
        String id = ""+makeid(key);
        
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "*:*");
        solrParams.set("fq", "id:\"" + id + "\"");
        QueryResponse res = 
            solrServer.query(solrParams);
        
        JSONObject msg = null;
        for(SolrDocument doc : res.getResults()) {
            msg = jsonizeSolrDoc(doc);
        } // TODO: anti-entropy; for now, always take the last written
        if (msg == null) return null;
        
        JSONObject riakObject = makeRiakObject(msg);
        
        rates.add("mecha.db.bucket.global.get");
        
        return riakObject.toString().getBytes();
    }
    
    private JSONObject jsonizeSolrDoc(SolrDocument doc) throws Exception {
        JSONObject msg = new JSONObject();
        for(String fieldName : doc.getFieldNames()) {
            if (fieldName.endsWith("_dt")) {
                String date = 
                    dateFormat.format((Date)doc.get(fieldName));
                msg.put(fieldName, date);
            } else {
                msg.put(fieldName, doc.get(fieldName));
            }
        }
        return msg;
    }
    
    /*
     * msg: "raw" translation of solr document to json equivalent
    */
    private JSONObject makeRiakObject(JSONObject msg) throws Exception {
        JSONObject riakObject = new JSONObject();
        JSONArray values = new JSONArray();
        
        riakObject.put("bucket", msg.getString("bucket"));
        riakObject.put("key", msg.getString("key"));
        riakObject.put("vclock", msg.getString("vclock"));
        
        JSONObject obj = new JSONObject();
        JSONObject md = new JSONObject();
        md.put("links", new JSONArray());
        md.put("Content-type", "text/json");
        md.put("X-Riak-VTag", msg.getString("vtag"));
        md.put("index", new JSONArray());
        md.put("X-Riak-Last-Modified", msg.getString("last_modified"));
        md.put("X-Riak-Meta", new JSONArray());
        obj.put("metadata", md);
        
        msg.remove("bucket");
        msg.remove("key");
        if (msg.has("partition")) msg.remove("partition");
        msg.remove("last_modified");
        msg.remove("vclock");
        msg.remove("vtag");
        msg.remove("id");
        msg.remove("h2");
        
        obj.put("data", msg.toString());
        values.put(obj);
        riakObject.put("values", values);
        
        return riakObject;
    }
    
    private JSONObject solr2riak(SolrDocument doc) throws Exception {
        return makeRiakObject(jsonizeSolrDoc(doc));
    }
    
    public void put(byte[] key, byte[] value) throws Exception {
        try {
            rates.add("mecha.db.bucket.global.put");
            JSONObject obj = new JSONObject(new String(value));
            JSONArray values = obj.getJSONArray("values");
            
            /*
             *
             * TODO: intelligently, consistently handle siblings (or disable them entirely?)
             *
             * CURRENT: index only the "last written value"
             *
            */
            JSONObject jo1 = values.getJSONObject(values.length()-1);

            /*
             *
             * If the object has been deleted via any concurrent process, remove object record & index entry
             *
            */
            if (jo1.has("metadata") &&
                jo1.getJSONObject("metadata").has("X-Riak-Deleted")) {
                if (jo1.getJSONObject("metadata").getString("X-Riak-Deleted").equals("true")) {
                    /*
                     * TODO: force cluster-wide delete? (though this still doesn't solve the tombstone/timer issue)
                    */
                    delete(key);
                    return;
                }
            }
            
            final String vclock = obj.getString("vclock");
            String vtag = jo1.getJSONObject("metadata").getString("X-Riak-VTag");
            String last_modified = jo1.getJSONObject("metadata").getString("X-Riak-Last-Modified");
            
            final JSONObject jo;
            
            try {
                jo = new JSONObject(jo1.getString("data"));
            } catch (Exception encEx) {
                // cannot encode to json, unknown format, simply
                //  save as binary & do not index.
                
                // TODO: store full object in a _binary object payload in
                //       solr-- use external storage if possible?
                //db.put(key, value);
                return;
            }
            
            /*
             * Because the object is not deleted, write to object store.
            */
            
            //
            // TODO: PUT with vclock, vtag, last_modified from riak_object
            //
            
            //db.put(key, value);
                        
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", ""+makeid(key));
            //doc.addField("h2", ""+makeh2(key));
            //doc.addField("partition", partition);
            doc.addField("bucket", bucketStr);
            doc.addField("key", new String(key));
            doc.addField("vclock", vclock);
            doc.addField("vtag", vtag);
            doc.addField("last_modified", last_modified);
            
            for(String f: JSONObject.getNames(jo)) {
                
                // TODO: configuration driven w/ indexing/indexer plugins
                
                if (
                    
                    /*
                     * stored & indexed
                    */
                    
                    f.endsWith("_s") ||     // exact string
                    f.endsWith("_dt") ||    // ISO8601 date
                    f.endsWith("_t") ||     // fulltext (default analysis, no vectors)
                    f.endsWith("_tt") ||    // fulltext (default analysis, vectors)
                    f.endsWith("_i") ||     // integer
                    f.endsWith("_l") ||     // long
                    f.endsWith("_f") ||     // float
                    f.endsWith("_d") ||     // double
                    f.endsWith("_b") ||     // boolean ("true" or "false")
                    f.endsWith("_xy") ||    // x,y coordinate
                    f.endsWith("_xyz") ||    // x,y,z coordinate
                    f.endsWith("_xyzw") ||    // x,y,z,w coordinate
                    f.endsWith("_ll") ||    // lat,lon latitude, longitude coordinate
                    f.endsWith("_geo") ||
                    f.endsWith("_cat") ||
                    f.endsWith("_cur") ||
                    
                    /*
                     * stored, not indexed
                    */
                    
                    f.endsWith("_ss") ||     // exact string
                    f.endsWith("_sdt") ||    // ISO8601 date
                    f.endsWith("_st") ||     // fulltext (default analysis, no vectors)
                    f.endsWith("_stt") ||    // fulltext (default analysis, vectors)
                    f.endsWith("_si") ||     // integer
                    f.endsWith("_sl") ||     // long
                    f.endsWith("_sf") ||     // float
                    f.endsWith("_sd") ||     // double
                    f.endsWith("_sb") ||     // boolean ("true" or "false")
                    f.endsWith("_sxy") ||    // x,y coordinate
                    f.endsWith("_sxyz") ||    // x,y,z coordinate
                    f.endsWith("_sxyzw") ||    // x,y,z,w coordinate
                    f.endsWith("_sll") ||    // lat,lon latitude, longitude coordinate
                    f.endsWith("_sgeo") ||
                    f.endsWith("_scur") ||
                    
                    /*
                     * indexed, not stored
                    */
                    
                    f.endsWith("_is") ||     // exact string
                    f.endsWith("_idt") ||    // ISO8601 date
                    f.endsWith("_it") ||     // fulltext (default analysis, no vectors)
                    f.endsWith("_itt") ||    // fulltext (default analysis, vectors)
                    f.endsWith("_ii") ||     // integer
                    f.endsWith("_il") ||     // long
                    f.endsWith("_if") ||     // float
                    f.endsWith("_id") ||     // double
                    f.endsWith("_ib") ||     // boolean ("true" or "false")
                    f.endsWith("_ixy") ||    // x,y coordinate
                    f.endsWith("_ixyz") ||    // x,y,z coordinate
                    f.endsWith("_ixyzw") ||    // x,y,z,w coordinate
                    f.endsWith("_ill") ||    // lat,lon latitude, longitude coordinate
                    f.endsWith("_igeo") ||
                    f.endsWith("_icur") || 
                    f.endsWith("_icat") ||
                    
                    /*
                     * stored binary value
                    */
                    
                    f.endsWith("_bin")        // binary stored field
                        ) {
                    
                    if (f.endsWith("_dt")) {
                        if (!jo.getString(f).endsWith("Z")) {
                            jo.put(f,jo.getString(f) + "Z");
                        }
                    }
                    
                    doc.addField(f, jo.get(f));
                
                /*
                 * multi-value fields
                */
                
                } else if (f.endsWith("_s_mv") ||       // stored, indexed, array of strings
                           f.endsWith("_ss_mv") ||      // stored, not indexed, array of strings
                           f.endsWith("_is_mv")         // indexed, not stored, array of strings
                        ) {   // array of exact strings
                    JSONArray mv = jo.getJSONArray(f);
                    List<String> vals = new ArrayList<String>();
                    for(int j=0; j<mv.length(); j++) {
                        vals.add(mv.getString(j));
                    }    
                    doc.addField(f, vals);
                }
            }
            solrServer.add(doc);
        
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            ex.printStackTrace();
            log.info("Bucket: put: " + new String(key) + ": <value = " + new String(value) + ">");
            throw ex;
        }
    }
    
    public void delete(byte[] key) throws Exception {
        log.info(partition + ": delete: " + (new String(key)));
        try {
            rates.add("mecha.db.bucket.global.delete");
            solrServer.deleteByQuery("id:\"" + makeid(key) + "\"");
            solrServer.commit();
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            /*
             * Any other error should be rethrown.  Any exception here
             *  indicates an error during either server.deleteByQuery (in
             *  which case the Solr server is broken in some way), or 
             *  any error other than key not found in the leveldb store;
             *  ultimately, this presents a discontinuity between the
             *  store and the index and should be handled by bubbling
             *  the error up to the client so they may act to resolve
             *  what amounts to a failed delete.
            */
            ex.printStackTrace();
            throw ex;
        }
    }
    
    public boolean foreach(final MDB.ForEachFunction forEachFunction) throws Exception {
        rates.add("mecha.db.bucket.global.foreach");

        final Semaphore streamSemaphore = 
            new Semaphore(1, true);
        streamSemaphore.acquire();
        
        final String q = 
            "partition:" + partition + 
            " AND bucket:" + bucketStr;
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "*:*");
        solrParams.set("fq", q);
        
        final AtomicBoolean fe_b = new AtomicBoolean(true);

        QueryResponse res = 
            solrServer.queryAndStreamResponse(solrParams, new StreamingResponseCallback() {
                long numFound = -1;
                long count = 0;
                
                public void streamDocListInfo(long numFound,
                                              long start,
                                              Float maxScore) {
                    log.info("streamDocListInfo: numFound = " + numFound + ", start = " + start + ", maxScore = " + maxScore);
                    this.numFound = numFound;
                }
                
                public void streamSolrDocument(SolrDocument doc) {
                    if (!fe_b.get()) return;
                    
                    count++;
                    //log.info(count + ": streamSolrDocument(" + doc + ")");
                    
                    try {
                        final JSONObject solrObj = jsonizeSolrDoc(doc);
                        final byte[] key = solrObj.getString("key").getBytes();
                        final JSONObject riakObj = makeRiakObject(solrObj);
                        final byte[] value = riakObj.toString().getBytes();
                        
                        if (!forEachFunction.each(bucket, key, value)) {
                            fe_b.set(false);
                            //log.info("releasing stream semaphore");
                            streamSemaphore.release();
                            return;
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    if (count == numFound) {
                        //log.info("end of line; automatically releasing stream semaphore");
                        streamSemaphore.release();
                    }
                }
        });
        
        streamSemaphore.acquire();
        return fe_b.get();

        //return db.foreach(forEachFunction);
    }
    
    public long count() throws Exception {
        rates.add("mecha.db.bucket.global.count");
        final String q = 
            "partition:" + partition + 
            " AND bucket:" + bucketStr;
            
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "*:*");
        solrParams.set("fq", q);
        solrParams.set("rows", 0);
        QueryResponse res = 
            solrServer.query(solrParams);
        return res.getResults().getNumFound();
    }
    
    public boolean isEmpty() throws Exception {
        rates.add("mecha.db.bucket.global.is-empty");
        
        return count() == 0;
    }
    
    public synchronized void drop() throws Exception {
        rates.add("mecha.db.bucket.global.drop");
        try {
            solrServer.deleteByQuery(
                "partition:" + partition + 
                " AND bucket:" + bucketStr);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            ex.printStackTrace();
        }
    }
    
    private int makeid(final byte[] key) throws Exception {
        byte[] hashval = String.format("%1$s,%2$s,%3$s",
                partition, bucketStr, new String(key)).getBytes();
        return MurmurHash3.murmurhash3_x86_32(
            hashval, 0, hashval.length, MURMUR_SEED);
        
        /*
        return HashUtils.sha1(
            String.format("%1$s,%2$s,%3$s",
                partition, bucketStr, new String(key)));
        */
    }
    
    private int makeh2(final byte[] key) throws Exception {
        byte[] hashval = String.format("%1$s,%2$s",
                bucketStr, new String(key)).getBytes();
        return MurmurHash3.murmurhash3_x86_32(
            hashval, 0, hashval.length, MURMUR_SEED);
        
        /*
        return HashUtils.sha1(
            String.format("%1$s,%2$s,%3$s",
                partition, bucketStr, new String(key)));
        */
    }
    
    public String getBucketName() {
        return bucketStr;
    }
    
    public void commit() {
        //log.info("<faux commit>");
        //db.commit();
    }
}

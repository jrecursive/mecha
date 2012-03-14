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
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.logging.*;
import org.fusesource.leveldbjni.*;
import static org.fusesource.leveldbjni.DB.*;
import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;

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
    
    final private byte[] bucket;
    final private String bucketStr;
    final private String partition;
    final private String dataDir;
    final private String bucketDriverClassName;
    final private BucketDriver db;
    final private SolrServer solrServer;
    
    // solr
    final private SolrServer server;
    final private LinkedBlockingQueue<SolrInputDocument> solrDocQueue;
    
    public Bucket(String partition, 
                  byte[] bucket, 
                  String dataDir,
                  SolrServer server,
                  LinkedBlockingQueue<SolrInputDocument> solrDocQueue) 
                    throws Exception {
        this.partition = partition;
        this.bucket = bucket;
        this.bucketStr = (new String(bucket)).trim();
        this.dataDir = dataDir;
        this.server = server;
        this.solrDocQueue = solrDocQueue;
        this.bucketDriverClassName = Mecha.getConfig().<String>get("bucket-driver");
        
        Class driverClassObj = Class.forName(bucketDriverClassName);
        Class[] argTypes = { String.class, String.class, String.class };
        Object[] args = { partition, bucketStr, dataDir };
        db = (BucketDriver) driverClassObj.getConstructor(argTypes).newInstance(args);
        solrServer = Mecha.getSolrManager().getCore("index").getServer();
        
        log.info("[" + bucketDriverClassName + "] " + 
            "Bucket: " + partition + ": " + bucketStr + ": " + dataDir);
    }
    
    private void queueForIndexing(SolrInputDocument doc) throws Exception {
        //solrDocQueue.put(doc);
        solrServer.add(doc);
    }
    
    public void stop() throws Exception {
        db.stop();
    }
    
    public byte[] get(byte[] key) throws Exception {
        rates.add("mecha.db.bucket.global.get");
        return db.get(key);
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
             * CURRENT: index only the "most current" value
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
                     * TODO: force cluster-wide delete?
                    */
                    delete(key);
                    return;
                }
            }

            JSONObject jo = new JSONObject(jo1.getString("data"));
            
            /*
             * Because the object is not deleted, write to object store.
            */
            db.put(key, value);
                        
            String id = makeid(key);
            
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", id);
            doc.addField("partition", partition);
            doc.addField("bucket", bucketStr);
            doc.addField("key", new String(key));
            
            for(String f: JSONObject.getNames(jo)) {
                
                // TODO: configuration driven w/ indexing/indexer plugins
                
                if (f.endsWith("_s") ||     // exact string
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
                    f.endsWith("_geo") ||   // geohash
                    
                    
                    /*
                     * If last_modified is specifically set as a field on
                     *  a PUT, index the value specified -- this is required
                     *  so it is not "reset" during handoff.
                    */
                    f.equals("last_modified")
                ) {
                    doc.addField(f, jo.get(f));
                } else if (f.endsWith("_s_mv")) {   // array of exact strings
                    JSONArray mv = jo.getJSONArray(f);
                    List<String> vals = new ArrayList<String>();
                    for(int j=0; j<mv.length(); j++) {
                        vals.add(mv.getString(j));
                    }
                    
                    doc.addField(f, vals);
                }
            }
            queueForIndexing(doc);
        
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
            server.deleteByQuery(
                "bucket:\"" + bucketStr + "\" AND " +
                "key:\"" + (new String(key)) + "\"");
            db.delete(key);
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
    
    public boolean foreach(MDB.ForEachFunction forEachFunction) throws Exception {
        rates.add("mecha.db.bucket.global.foreach");
        return db.foreach(forEachFunction);
    }
    
    public long count() throws Exception {
        rates.add("mecha.db.bucket.global.count");
        return db.count();
    }
    
    public boolean isEmpty() throws Exception {
        rates.add("mecha.db.bucket.global.is-empty");
        return db.isEmpty();
    }
    
    public synchronized void drop() throws Exception {
        rates.add("mecha.db.bucket.global.drop");
        try {
            server.deleteByQuery(
                "partition:" + partition + 
                " AND bucket:" + bucketStr);
            db.drop();
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            ex.printStackTrace();
        }
    }
    
    private String makeid(byte[] key) throws Exception {
        return HashUtils.sha1(
            String.format("%1$s,%2$s,%3$s",
                partition, bucketStr, new String(key)));
    }
    
    public String getBucketName() {
        return bucketStr;
    }
    
    public void commit() {
        db.commit();
    }
}

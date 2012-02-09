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
import mecha.json.*;

import mecha.Mecha;
import mecha.util.*;

public class Bucket {
    final private static Logger log = 
        Logger.getLogger(Bucket.class.getName());
    
    private byte[] bucket;
    private String bucketStr;
    private String partition;
    private String dataDir;
    
    // leveldb
    DB db;
    
    // solr
    final private SolrServer server;
    final private LinkedBlockingQueue<SolrInputDocument> solrDocQueue;
    
    // config
    final private boolean syncOnWrite;
    
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
        syncOnWrite = Mecha.getConfig().getJSONObject("leveldb").<Boolean>get("sync-on-write");
        
        Options options = new Options()
            .createIfMissing(true)
            .cache(new Cache(Mecha.getConfig().getJSONObject("leveldb").getInt("cache-per-bucket")))
            .compression(CompressionType.kSnappyCompression);
        log.info("Bucket: " + partition + ": " + bucketStr + ": " + dataDir);
        db = DB.open(options, new File(dataDir));
    }
    
    private WriteOptions getWriteOptions() {
        return new WriteOptions().sync(syncOnWrite);
    }
    
    private void queueForIndexing(SolrInputDocument doc) throws Exception {
        solrDocQueue.put(doc);
    }
    
    public void stop() throws Exception {
        // deletes the JNI-bound object (not the db)
        db.delete();
    }
    
    public byte[] get(byte[] key) throws Exception {
        try {
            return db.get(new ReadOptions(), key);
        } catch (DBException notFound) {
            return null;
        }
    }
    
    public void put(byte[] key, byte[] value) throws Exception {
        try {
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
            JSONObject jo = new JSONObject(jo1.getString("data"));

            /*
             *
             * If the object has been deleted via any concurrent process, remove object record & index entry
             *
            */
            if (jo1.has("metadata") &&
                jo1.getJSONObject("metadata").has("X-Riak-Deleted")) {
                if (jo1.getJSONObject("metadata").getString("X-Riak-Deleted").equals("true")) {
                    delete(key);
                    return;
                }
            }
            
            /*
             * Because the object is not deleted, write to object store.
            */
            db.put(getWriteOptions(), key, value);
                        
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
            ex.printStackTrace();
            log.info("Bucket: put: " + new String(key) + ": <value = " + new String(value) + ">");
            throw ex;
        }
    }
    
    public void delete(byte[] key) throws Exception {
        log.info(partition + ": delete: " + (new String(key)));
        try {
            server.deleteByQuery("id:" + makeid(key));
            try {
                db.delete(getWriteOptions(), key);
            } catch (DBException notFound) {
                /*
                 * TODO: analysis of discontinuity scenarios
                 *       where solr delete succeeds (0 or more) &
                 *       leveldb reports key not found; does this
                 *       warrant any action other than to log it
                 *       and otherwise ignore?
                */
            }
        } catch (Exception ex) {
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
        ReadOptions ro = null;
        Iterator iterator = null;
        try {
            boolean itrsw = true;
            ro = new ReadOptions().snapshot(db.getSnapshot());
            iterator = db.iterator(ro);
            for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                if (!forEachFunction.each(bucket, iterator.key(), iterator.value())) {
                    itrsw = false;
                    break;
                }
            }
            return itrsw;
        } finally {
            if (null != iterator) {
                iterator.delete();
            }
            if (null != ro) {
                db.releaseSnapshot(ro.snapshot());
                ro.snapshot(null);
            }
        }
    }
    
    public long count() throws Exception {
        ReadOptions ro = null; 
        Iterator iterator = null; 
        long ct = 0;
        try {
            ro = new ReadOptions().snapshot(db.getSnapshot());
            iterator = db.iterator(ro);
            for(iterator.seekToFirst(); iterator.isValid(); iterator.next())
                ct++;
        } finally {
            if (null != iterator) {
                iterator.delete();
            }
            if (null != ro) {
                db.releaseSnapshot(ro.snapshot());
                ro.snapshot(null);
            }
        }
        return ct;
    }
    
    public boolean isEmpty() throws Exception {
        return count() == 0;
    }
    
    public void drop() throws Exception {
        db.delete();
        File rc = new File(dataDir);
        deleteFile(rc);
        try {
            server.deleteByQuery("partition:" + partition);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void deleteFile(File rc) throws Exception {
        if (rc.isDirectory()) {
            log.info(bucketStr + ": deleteFile: directory: " + rc.getCanonicalPath());
            for (File f : rc.listFiles()) {
                log.info(bucketStr + ": deleteFile: file: " + f.getCanonicalPath());
                deleteFile(f);
            }
        }
        log.info("delete: " + rc.toString());
        rc.delete();
    }
    
    private String makeid(byte[] key) throws Exception {
        return HashUtils.sha1(
            String.format("%1$s,%2$s,%3$s",
                partition, bucketStr, new String(key)));
    }
}

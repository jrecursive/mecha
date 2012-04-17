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
import java.util.logging.*;
import java.net.URL;
import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.response.UpdateResponse;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;
import mecha.monitoring.*;

public class MDB {
    final private static Logger log = 
        Logger.getLogger(MDB.class.getName());
    
    final static private Rates rates = new Rates();
        
    // predicates
    public interface ForEachFunction {
        public boolean each(byte[] bucket, byte[] key, byte[] value);
    }
    
    // maps partitions to Bucket instances
    final private Map<String, Map<String, Bucket>> partitionBuckets =
        new ConcurrentHashMap<String, Map<String, Bucket>>();
        
    // maps partitions to absolute disk locations
    final private Map<String, String> partitionDirs =
        new ConcurrentHashMap<String, String>();

    // queue of documents to be indexed
    final private LinkedBlockingQueue<SolrInputDocument> solrDocumentQueue =
        new LinkedBlockingQueue<SolrInputDocument>();

    //private SolrServer solrServer;
        
    public MDB() throws Exception { }
    
    public void startMDB() throws Exception {
        //solrServer = Mecha.getSolrManager().getCore("index").getServer();
        //log.info("solrServer: " + solrServer.toString());
                
        Mecha.getMonitoring().addMonitoredRates(rates);
        log.info("started");
    }
    
    public synchronized void commit() throws Exception {
        Mecha.getMonitoring().log("mecha.db.mdb", "starting object store commit");
        for(Map.Entry<String, Map<String, Bucket>> entry : partitionBuckets.entrySet()) {
            for(Map.Entry <String, Bucket> bucketEntry : entry.getValue().entrySet()) {
                log.info("commit: " + entry.getKey() + ": " + bucketEntry.getKey());
                bucketEntry.getValue().commit();
            }
        }
        Mecha.getMonitoring().log("mecha.db.mdb", "object store commit complete");
        log.info("object store commit complete");
    }
    
    public Set<String> getActivePartitions() throws Exception {
        return partitionBuckets.keySet();
    }
    
    public Set<String> getPartitionBuckets(String partition) throws Exception {
        return partitionBuckets.get(partition).keySet();
    }
    
    public void shutdown() throws Exception {
        log.info("forcing MDB commit for shutdown...");
        commit();
        for(String partition : getActivePartitions()) {
            System.out.println("stopping partition " + partition);
            stop(partition);
        }
        System.out.println("mdb stopped.");
    }
    
    
    /* 
     * storage
    */
    
    public synchronized void start(String partition) throws Exception {
        try {
            String dataDirRoot = Mecha.getConfig().getString("data-directory");
            String dataDir = dataDirRoot + "/" + partition;
            new File(dataDir).mkdirs();
            partitionDirs.put(partition, dataDir);
            openBuckets(partition, dataDir);
            log.info("start: " + partition + " -> " + dataDir);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            ex.printStackTrace();
        }
    }
    
    public void stop(String partition) throws Exception {
        if (null == partitionBuckets.get(partition)) return;
        for(Bucket bw: partitionBuckets.get(partition).values()) {
            System.out.println("stop: " + partition + ": " + bw.getBucketName());
            bw.stop();
        }
        partitionBuckets.remove(partition);
        System.out.println("stop: " + partition);
        rates.add("mecha.db.mdb.stop");
    }
    
    public byte[] get(String partition, byte[] bucket, byte[] key) throws Exception {
        rates.add("mecha.db.mdb.get");
        if (null == partitionBuckets.get(partition)) start(partition);
        return getBucket(partition, bucket).get(key);
    }
    
    public void put(String partition, byte[] bucket, byte[] key, byte[] value) throws Exception {
        rates.add("mecha.db.mdb.put");
        if (null == partitionBuckets.get(partition)) start(partition);
        getBucket(partition, bucket).put(key, value);
    }
    
    public void delete(String partition, byte[] bucket, byte[] key) throws Exception {
        rates.add("mecha.db.mdb.delete");
        if (null == partitionBuckets.get(partition)) start(partition);
        getBucket(partition, bucket).delete(key);
    }
    
    public void fold(String partition, 
                        MDB.ForEachFunction forEachFunction) throws Exception {
        rates.add("mecha.db.mdb.fold");
        log.info("fold: " + partition);
        if (null == partitionBuckets.get(partition)) start(partition);
        Map<String, Bucket> pbWMap = partitionBuckets.get(partition);
        for(String b: pbWMap.keySet()) {
            if (!partitionBuckets
                    .get(partition)
                    .get(b)
                    .foreach(forEachFunction))
                break;
        }
    }
    
    public void foldBucketNames(String partition, 
                                MDB.ForEachFunction fun) throws Exception {
        rates.add("mecha.db.mdb.fold-bucket-names");
        if (null == partitionBuckets.get(partition)) start(partition);
        Map<String, Bucket> pbWMap = partitionBuckets.get(partition);
        for(String b: pbWMap.keySet()) {
            fun.each(b.getBytes(), null, null);
        }
    }
    
    public boolean foldBucket(String partition, 
                        byte[] bucket, 
                        MDB.ForEachFunction forEachFunction) throws Exception {
        rates.add("mecha.db.mdb.fold-bucket");
        if (null == partitionBuckets.get(partition)) start(partition);
        String b = new String(bucket);
        log.info("foldBucket: " + partition + ": " + b);
        Bucket bw = getBucket(partition, bucket);
        return bw.foreach(forEachFunction);
    }
    
    public synchronized boolean isEmpty(String partition) throws Exception {
        rates.add("mecha.db.mdb.is-empty");
        log.info("isEmpty: " + partition);
        if (null == partitionBuckets.get(partition)) start(partition);
        if (null == partitionBuckets.get(partition)) {
            log.info("partitionBuckets.get(" + partition + ") == null: isEmpty -> true");
            return true;
        }
        if (null == partitionBuckets.get(partition).keySet()) {
            log.info("partitionBuckets.get(" + partition + ").keySet() == null: isEmpty -> true");
            return true;
        }
        log.info("isEmpty: " + partition + ": iterating.. ");
        for(String b: partitionBuckets.get(partition).keySet()) {
            if (!partitionBuckets.get(partition).get(b).isEmpty()) {
                log.info("isEmpty: " + partition + ": false! Not empty (" + b + ")");
                return false;
            }
        }
        return true;
    }
    
    public synchronized void drop(String partition) throws Exception {
        rates.add("mecha.db.mdb.drop");
        log.info("drop: " + partition);
        if (null == partitionBuckets.get(partition)) start(partition);
        if (null == partitionBuckets.get(partition)) {
            log.info("partitionBuckets.get(" + partition + ") == null: drop: early return (ok)");
            return;
        }
        for(String b: partitionBuckets.get(partition).keySet()) {
            Bucket bw = partitionBuckets.get(partition).get(b);
            log.info("drop: " + partition + ": " + b);
            bw.drop();
        }
    }
    
    // TODO: consolidate drop / dropBucket overlap
    
    public synchronized int globalDropBucket(String bucket) throws Exception {
        rates.add("mecha.db.mdb.global-drop-bucket");
        log.info("globalDropBucket: bucket: " + bucket + ": starting...");
        int count = 0;
        synchronized(partitionBuckets) {
            for(String partitionKey : new HashMap<String, Map<String,Bucket>>(partitionBuckets).keySet()) {
                dropBucket(partitionKey, bucket.getBytes());
                log.info("globalDropBucket: partition: " + partitionKey + 
                    "bucket: " + bucket);
                count++;
            }
            /*
            if (count > 0) {
                log.info("globalDropBucket: " + bucket + ": Removing from index...");
                solrServer.deleteByQuery("bucket:" + bucket);
                log.info("globalDropBucket: " + bucket + ": Commiting changes...");
                solrServer.commit(true,true);
                log.info("globalDropBucket: " + bucket + ": done!");
            }
            */
        }
        return count;
    }
    
    public synchronized void dropBucket(String partition, byte[] bucket) throws Exception {
        rates.add("mecha.db.mdb.drop-bucket");
        log.info("dropBucket: partition: " + partition + " bucket: " + new String(bucket));
        if (null == partitionBuckets.get(partition)) start(partition);
        if (null == partitionBuckets.get(partition)) {
            log.info("partitionBuckets.get(" + partition + ") == null: " + 
                "dropBucket <" + (new String(bucket)) + ">: no partition: early return (ok)");
            return;
        }
        Bucket bw = getBucket(partition, bucket);
        if (bw == null) {
            log.info("partitionBuckets.get(" + partition + ").get(" + bucket + 
                ") == null: dropBucket <" + (new String(bucket)) + ">: no bucket! early return (ok)");
            return;
        } else {
            log.info("dropBucket: partition:" + partition + ": " + bucket);
            synchronized(bw) {
                bw.drop();
            }
        }
    }
    
    // TODO: diffuse & join threading
    
    public List<byte[][]> listKeys(String partition) throws Exception {
        rates.add("mecha.db.mdb.list-keys");
        log.info("listKeys: " + partition);
        if (null == partitionBuckets.get(partition)) start(partition);
        final List<byte[][]> keys = new ArrayList<byte[][]>();
        for(String b: partitionBuckets.get(partition).keySet()) {
            log.info("listKeys: " + partition + ": iterating " + b);
            Bucket bw = partitionBuckets.get(partition).get(b);
            bw.foreach(new MDB.ForEachFunction() {
                            public boolean each(byte[] bucket, byte[] key, byte[] val) {
                                byte[][] entry = { bucket, key };
                                keys.add(entry);
                                return true;
                            }});
        }
        return keys;
    }
    
    public List<byte[]> listKeys(String partition, byte[] bucket) throws Exception {
        rates.add("mecha.db.mdb.list-keys");
        log.info("listKeys: " + partition + ": " + (new String(bucket)));
        if (null == partitionBuckets.get(partition)) start(partition);
        final List<byte[]> keys = new ArrayList<byte[]>();
        Bucket bw = getBucket(partition, bucket);
        bw.foreach(new MDB.ForEachFunction() {
                        public boolean each(byte[] bucket, byte[] key, byte[] val) {
                            keys.add(key);
                            return true;
                        }});
        return keys;
    }

    /*
     * helpers
    */
    
    public Bucket getBucket(String partition, String bucket) throws Exception {
        return getBucket(partition, bucket.getBytes());
    }

    public Bucket getBucket(String partition, byte[] bucket) throws Exception {
        if (null == partitionDirs.get(partition)) start(partition);
        String b = new String(bucket);
        Map<String, Bucket> bucketMap = partitionBuckets.get(partition);
        if (null == bucketMap) bucketMap = new ConcurrentHashMap<String, Bucket>();
        if (null == bucketMap.get(b)) {
            String bEnc = HashUtils.sha1(b);
            String bucketDataDir = partitionDirs.get(partition) + "/" + bEnc;
            String mdFn = bucketDataDir + ".bucket";
            String bucketName = TextFile.get(mdFn);
            if (null == bucketName) {
                TextFile.put(mdFn, new String(bucket, "UTF-8"));
            }
            Bucket bw = new Bucket(partition, bucket, bucketDataDir);
            bucketMap.put(b, bw);
            partitionBuckets.put(partition, bucketMap);
            return bw;
        } else {
            return bucketMap.get(b);
        }
    }

    private void openBuckets(String partition, String pDir) throws Exception {
        File rc = new File(pDir);
        if (rc.isDirectory()) {
            for (File f : rc.listFiles()) {
                if (f.toString().endsWith(".bucket")) {
                    byte[] bucket = TextFile.get(f.toString()).getBytes("UTF-8");
                    getBucket(partition, bucket); // TODO don't initialize by side effect
                }
            }
        }
    }
}

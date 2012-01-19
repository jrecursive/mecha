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
import org.json.*;

import mecha.Mecha;
import mecha.util.*;

public class MDB {
    final private static Logger log = 
        Logger.getLogger(MDB.class.getName());
        
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
    
    // Solr committer
    final private URL solrUrl;
    final private SolrServer solrServer;
    final private Thread documentQueueIndexerThread;
    final LinkedBlockingQueue<SolrInputDocument> solrDocumentQueue =
        new LinkedBlockingQueue<SolrInputDocument>();
    
    private class DocumentQueueIndexer implements Runnable {
        public void run() {
            while(true) {
                try {
                    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
                    solrDocumentQueue.drainTo(docs);
                    if (docs.size() > 0) {
                        solrServer.add(docs);
                        log.info(docs.size() + " indexed");
                    }
                    Thread.sleep(100);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    public MDB() throws Exception {
        solrUrl = new URL(Mecha.getConfig().getString("solr-url"));
        solrServer = new CommonsHttpSolrServer(solrUrl);
        log.info("solrUrl = " + solrUrl + " ping: " + solrServer.ping());
        documentQueueIndexerThread = 
            new Thread(new DocumentQueueIndexer());
        documentQueueIndexerThread.start();
        log.info("started");
    }
    
    /* 
     * storage methods
    */
    
    public void start(String partition) throws Exception {
        // TODO: Consistently distribute data across all named directories (if array of names configured)
        try {
            String dataDirRoot = Mecha.getConfig().getString("data-directory");
            String dataDir = dataDirRoot + File.pathSeparator + partition;
            new File(dataDir).mkdirs();
            partitionDirs.put(partition, dataDir);
            openBuckets(partition, dataDir);
            log.info("start: " + partition + " -> " + dataDir);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void stop(String partition) throws Exception {
        if (null == partitionBuckets.get(partition)) return;
        for(Bucket bw: partitionBuckets.get(partition).values()) {
            bw.stop();
        }
        partitionBuckets.remove(partition);
        log.info("stop: " + partition);
    }
    
    public byte[] get(String partition, byte[] bucket, byte[] key) throws Exception {
        if (null == partitionBuckets.get(partition)) start(partition);
        return getBucket(partition, bucket).get(key);
    }
    
    public void put(String partition, byte[] bucket, byte[] key, byte[] value) throws Exception {
        if (null == partitionBuckets.get(partition)) start(partition);
        getBucket(partition, bucket).put(key, value);
    }
    
    public void delete(String partition, byte[] bucket, byte[] key) throws Exception {
        //log.info("delete: " + (new String(bucket)) + ", " + (new String(key)));
        if (null == partitionBuckets.get(partition)) start(partition);
        getBucket(partition, bucket).delete(key);
    }
    
    public void fold(String partition, 
                        MDB.ForEachFunction forEachFunction) throws Exception {
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
        log.info("foldBucketNames: " + partition);
        if (null == partitionBuckets.get(partition)) start(partition);
        Map<String, Bucket> pbWMap = partitionBuckets.get(partition);
        for(String b: pbWMap.keySet()) {
            fun.each(b.getBytes(), null, null);
        }
    }
    
    public boolean foldBucket(String partition, 
                        byte[] bucket, 
                        MDB.ForEachFunction forEachFunction) throws Exception {
        if (null == partitionBuckets.get(partition)) start(partition);
        String b = new String(bucket);
        log.info("foldBucket: " + partition + ": " + b);
        Bucket bw = getBucket(partition, bucket);
        return bw.foreach(forEachFunction);
    }
    
    public synchronized boolean isEmpty(String partition) throws Exception {
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
    
    public synchronized void dropBucket(String partition, byte[] bucket) throws Exception {
        log.info("drop: " + partition);
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
            log.info("drop: " + partition + ": " + bucket);
            bw.drop();
        }
    }
    
    // TODO: diffuse & join threading
    
    public List<byte[][]> listKeys(String partition) throws Exception {
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
     * helper methods
    */

    private Bucket getBucket(String partition, byte[] bucket) throws Exception {
        if (null == partitionDirs.get(partition)) start(partition);
        String b = new String(bucket);
        Map<String, Bucket> bucketMap = partitionBuckets.get(partition);
        if (null == bucketMap) bucketMap = new ConcurrentHashMap<String, Bucket>();
        if (null == bucketMap.get(b)) {
            String bEnc = HashUtils.sha1(b);
            String bucketDataDir = partitionDirs.get(partition) + File.pathSeparator + bEnc;
            log.info("starting Bucket(" + partition + "," + bucketDataDir + ")");
            String mdFn = bucketDataDir + ".bucket";
            String bucketName = TextFile.get(mdFn);
            if (null == bucketName) {
                TextFile.put(mdFn, new String(bucket, "UTF-8"));
            }
            Bucket bw = new Bucket(partition, bucket, bucketDataDir, solrServer, solrDocumentQueue);
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
                    log.info("openBuckets: " + partition + ", " + pDir + " -> " + f.toString());
                    byte[] bucket = TextFile.get(f.toString()).getBytes("UTF-8");
                    getBucket(partition, bucket); // TODO don't initialize by side effect
                }
            }
        }
    }
    
}

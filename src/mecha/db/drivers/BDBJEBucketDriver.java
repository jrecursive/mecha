package mecha.db.drivers;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.logging.*;

import java.io.File;

import com.sleepycat.je.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;
import mecha.monitoring.*;
import mecha.db.MDB;
import mecha.db.BucketDriver;

public class BDBJEBucketDriver implements BucketDriver {
    final private static Logger log = 
        Logger.getLogger(BDBJEBucketDriver.class.getName());
    
    final private String bucketStr;
    final private String partition;
    final private String dataDir;
    final private File envDir;
    
    final private EnvironmentConfig envConfig;
    final private Environment env;
    final private DatabaseConfig dbConfig;
    final private Database db;
    private Transaction txn;
    
    public BDBJEBucketDriver(String partition, 
                             String bucketStr, 
                             String dataDir) throws Exception {
        this.partition = partition;
        this.bucketStr = bucketStr;
        this.dataDir = dataDir;
        envDir = new File(dataDir);
        envDir.mkdirs();
        
        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        
        env = new Environment(envDir, envConfig);
        txn = newTxn();
        
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(false);
        dbConfig.setKeyPrefixing(true);
        
        /*
         * TODO: configurable
        */
        dbConfig.setNodeMaxEntries(128);
        
        db = env.openDatabase(txn,
                              bucketStr,
                              dbConfig);
        txn.commit();
        txn = newTxn();
    }
    
    public void stop() throws Exception {
        // deletes the JNI-bound object (not the db)
        try {
            if (txn.isValid()) {
                log.info(bucketStr + ": committing closing transaction");
                txn.commit();
            }
            log.info(bucketStr + ": commit ok");
        } finally {
            log.info(bucketStr + ": closing database");
            db.close();
            log.info(bucketStr + ": closing environment");
            env.close();
        }
        log.info(bucketStr + ": stopped");
    }
    
    public byte[] get(byte[] key) throws Exception {
        DatabaseEntry data = new DatabaseEntry();
        /*
         * TODO: configurable LockMode:
         *  READ_UNCOMMITTED
         *  READ_COMMITTED
         *  DEFAULT
         *  RMW
        */
        OperationStatus status =
            db.get(null, 
                   dbentry(key),
                   data,
                   LockMode.READ_UNCOMMITTED);
        if (status == OperationStatus.SUCCESS) {
            return data.getData();
        } else if (status == OperationStatus.NOTFOUND) {
            return null;
        } else {
            throw new Exception(bucketStr + ": " +
                "get: unknown status: " + status);
        }
    }
    
    public void put(byte[] key, byte[] value) throws Exception {
        try {
            OperationStatus status;
            synchronized(txn) {
                status = db.put(txn, dbentry(key), dbentry(value));
            }
            if (status != OperationStatus.SUCCESS) {
                throw new RuntimeException(bucketStr + 
                    ": put: non-success status: " + status);
            }
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.mdb", ex);
            ex.printStackTrace();
            log.info("Bucket: put: " + new String(key) + ": <value = " + new String(value) + ">");
            throw ex;
        }
    }
    
    public void delete(byte[] key) throws Exception {
        log.info(partition + ": delete: " + (new String(key)));
        synchronized(txn) {
            db.delete(txn, dbentry(key));
        }
    }
    
    public boolean foreach(MDB.ForEachFunction forEachFunction) throws Exception {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        Cursor cursor = db.openCursor(null, null);
        try {
            boolean itrsw = true;
            while (cursor.getNext(keyEntry, dataEntry, LockMode.READ_UNCOMMITTED) ==
                OperationStatus.SUCCESS) {
                if (!forEachFunction.each(bucketStr.getBytes(), 
                                          keyEntry.getData(), 
                                          dataEntry.getData())) {
                    itrsw = false;
                    break;
                }
            }
            return itrsw;
        } finally {
            cursor.close();
        }
    }
    
    public long count() throws Exception {
        return db.count();
    }
    
    public boolean isEmpty() throws Exception {
        return count() == 0;
    }
    
    public synchronized void drop() throws Exception {
        log.info(bucketStr + ": drop(): stopping database");
        stop();
        log.info(bucketStr + ": drop(): deleting directory: " +
            dataDir);
        File rc = new File(dataDir);
        deleteFile(rc);
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
    
    public void commit() {
        log.info(bucketStr + ": commit(): synchronizing");
        synchronized(txn) {
            log.info(bucketStr + ": commit(): txn: " + txn);
            txn.commit();
            txn = newTxn();
            log.info(bucketStr + ": commit(): complete");
        }
    }
    
    /*
     * helpers...
    */
    
    private Transaction newTxn() {
        return env.beginTransaction(null, null);
    }
    
    private DatabaseEntry dbentry(byte[] bytes) throws Exception {
        return new DatabaseEntry(bytes);
    }
    
}

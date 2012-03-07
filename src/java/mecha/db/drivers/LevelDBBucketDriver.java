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

package mecha.db.drivers;

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
import mecha.db.MDB;
import mecha.db.BucketDriver;

public class LevelDBBucketDriver implements BucketDriver {
    final private static Logger log = 
        Logger.getLogger(LevelDBBucketDriver.class.getName());
    
    final private String bucketStr;
    final private String partition;
    final private String dataDir;
    
    // leveldb
    final private DB db;
        
    // config
    final private boolean syncOnWrite;
    
    public LevelDBBucketDriver(String partition, 
                               String bucketStr, 
                               String dataDir) throws Exception {
        this.partition = partition;
        this.bucketStr = bucketStr;
        this.dataDir = dataDir;
        syncOnWrite = Mecha.getConfig().getJSONObject("leveldb").<Boolean>get("sync-on-write");
        
        Options options = new Options()
            .createIfMissing(true)
            .cache(new Cache(Mecha.getConfig().getJSONObject("leveldb").getInt("cache-per-bucket")))
            .compression(CompressionType.kSnappyCompression);
        log.info("Bucket: " + partition + ": " + bucketStr + ": " + dataDir);
        
        synchronized(LevelDBBucketDriver.class) {
            db = DB.open(options, new File(dataDir));
        }
    }
    
    private WriteOptions getWriteOptions() {
        return new WriteOptions().sync(syncOnWrite);
    }
    
    public void stop() throws Exception {
        // deletes the JNI-bound object (not the db)
        db.delete();
    }
    
    public byte[] get(byte[] key) throws Exception {
        try {
            return db.get(new ReadOptions(), key);
        } catch (DBException notFound) {
            // do not log this message.
            return null;
        }
    }
    
    public void put(byte[] key, byte[] value) throws Exception {
        try {
            db.put(getWriteOptions(), key, value);        
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
            db.delete(getWriteOptions(), key);
        } catch (DBException notFound) {
            /*
             * TODO: analysis of discontinuity scenarios
             *       where solr delete succeeds (0 or more) &
             *       leveldb reports key not found; does this
             *       warrant any action other than to log it
             *       and otherwise ignore?
            */
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.db.store.level-db-driver", ex);
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
                if (!forEachFunction.each(bucketStr.getBytes(), iterator.key(), iterator.value())) {
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
    
    public synchronized void drop() throws Exception {
        db.delete();
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
        // not needed for leveldb (sync)
    }
}

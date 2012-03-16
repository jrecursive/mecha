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

package mecha.jinterface;

import java.lang.reflect.*; 
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.ericsson.otp.erlang.*;
import mecha.json.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.db.*;

public class RiakConnector extends OtpProcess {
    final private static Logger log = 
        Logger.getLogger(RiakConnector.class.getName());
    
    // jinterface
    
    final private Hashtable<String, Method> methodTable =
        new Hashtable<String, Method>();
    final private ExecutorService commandExecutor =
        Executors.newCachedThreadPool();
    
    // mdb

    final private MDB mdb;
    final private OtpProcessManager procMgr;
    
    public RiakConnector(MDB mdb) throws Exception {
        this.mdb = mdb;
        log.info(Mecha.getConfig().getString("mecha-nodename") + "/" + 
                 Mecha.getConfig().getString("riak-cookie"));
        procMgr = new OtpProcessManager(
            Mecha.getConfig().getString("mecha-nodename"),
            Mecha.getConfig().getString("riak-cookie"));
    }
    
    public void startConnector() throws Exception {
        procMgr.spawn("kv_store", this);
    }
    
    /*
     * OtpProcess methods
    */
    
    @Override
    public void receive(OtpMsg msg) {
        processMsg(msg);
    }
    
    private void processMsg(final OtpMsg msg) {
        commandExecutor.execute(new MsgRunnable(this,msg));
    }

    private class MsgRunnable implements Runnable {
        final public Object th;
        final public OtpMsg msg;
        
        public MsgRunnable(Object th, OtpMsg msg) {
            this.th = th;
            this.msg = msg;
        }
        
        public void run() {
            try {
                OtpErlangTuple svcMsg = (OtpErlangTuple) msg.getMsg();
                String cmd = ((OtpErlangAtom) svcMsg.elementAt(0)).atomValue();
                OtpErlangObject[] args = ((OtpErlangList) svcMsg.elementAt(1)).elements();
                OtpErlangPid replyToPid = (OtpErlangPid) svcMsg.elementAt(2);
                OtpErlangRef ref = (OtpErlangRef) svcMsg.elementAt(3);
                OtpErlangObject callResult = null;
                Method method;
                
                try {
                    String sigStr = cmd + "/" + args.length;
                    if ((method = methodTable.get(sigStr)) == null) {
                        Class[] sig = new Class[args.length];
                        for(int i=0; i<args.length; i++) {
                            sig[i] = (args[i]).getClass();
                        }
                        method = th.getClass().getDeclaredMethod(cmd, sig);
                        methodTable.put(sigStr, method);
                    }
                    callResult = (OtpErlangObject) method.invoke(th, (Object[])args);
                } catch (IllegalArgumentException illegalArgumentException) {
                    Mecha.getMonitoring().error("mecha.riak-connector", illegalArgumentException);
                    illegalArgumentException.printStackTrace();
                } catch (InvocationTargetException invocationTargetException) {
                    Mecha.getMonitoring().error("mecha.riak-connector", invocationTargetException);
                    invocationTargetException.printStackTrace();
                } catch (NoSuchMethodException noSuchMethodException) {
                    Mecha.getMonitoring().error("mecha.riak-connector", noSuchMethodException);
                    callResult = handle(msg);
                } catch (Exception ex) {
                    Mecha.getMonitoring().error("mecha.riak-connector", ex);
                    ex.printStackTrace();
                }
                
                if (callResult != null) {
                    OtpErlangObject[] reply = new OtpErlangObject[2];
                    reply[0] = ref;
                    reply[1] = callResult;
                    OtpErlangTuple r = new OtpErlangTuple(reply);
                    getMbox().send(replyToPid, r);
                }
            } catch (Exception ex) {
                Mecha.getMonitoring().error("mecha.riak-connector", ex);
                try {
                    System.out.println("msg = " + msg.getMsg());
                } catch (Exception ex2) {
                    Mecha.getMonitoring().error("mecha.riak-connector", ex2);
                }
                ex.printStackTrace();
            }
        }
    }
    
    public OtpErlangObject handle(OtpMsg msg) {
        try {
            log.info(msg.getSenderPid() + ": " + msg.getMsg());
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
        }
        return null;
    }
    
    /*
     * custom operations
    */
    
    public OtpErlangObject set_partition_callback_pid(OtpErlangLong partition, OtpErlangPid pid) {
        try {
            return new OtpErlangAtom("ok");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    /*
     * standard riak backend operations
    */
    
    public OtpErlangObject start(OtpErlangLong partition, OtpErlangPid callbackPid) {
        try {
            log.info(partition + ": starting");
            mdb.start(partition.toString());
            return new OtpErlangAtom("ok");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    public OtpErlangObject stop(OtpErlangLong partition, OtpErlangAtom _placeholder) {
        try {
            mdb.stop(partition.toString());
            //return new OtpErlangAtom("ok");
            return null;
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    public OtpErlangObject get(OtpErlangLong partition, OtpErlangBinary bucketBin, OtpErlangBinary keyBin) {
        try {
            byte[] bucket = bucketBin.binaryValue();
            byte[] key = keyBin.binaryValue();
            byte[] value;
            if ( (value = mdb.get(partition.toString(), bucket, key)) == null) {
                OtpErlangObject[] retval = { new OtpErlangAtom("error"), new OtpErlangAtom("not_found") };
                return new OtpErlangTuple(retval);
            } else {
                OtpErlangObject[] retval = { new OtpErlangAtom("ok"), new OtpErlangBinary(value) };
                return new OtpErlangTuple(retval);
            }
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }

    public OtpErlangObject put(OtpErlangLong partition, OtpErlangBinary bucketBin, OtpErlangBinary keyBin, OtpErlangBinary valueBin) {
        try {
            byte[] bucket = bucketBin.binaryValue();
            byte[] key = keyBin.binaryValue();
            byte[] value = valueBin.binaryValue();
            mdb.put(partition.toString(), bucket, key, value);
            return new OtpErlangAtom("ok");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    public OtpErlangObject delete(OtpErlangLong partition, OtpErlangBinary bucketBin, OtpErlangBinary keyBin) {
        try {
            byte[] bucket = bucketBin.binaryValue();
            byte[] key = keyBin.binaryValue();
            mdb.delete(partition.toString(), bucket, key);
            return new OtpErlangAtom("ok");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    /*
     * fold is currently a cast (no expected reply); results are sent directly to
     *  streamToPid.
    */
    public OtpErlangObject fold(OtpErlangLong partition, 
                                final OtpErlangPid streamToPid,
                                MDB.ForEachFunction forEachFunction) {
        return fold(partition, null, streamToPid, forEachFunction);
    }

    public OtpErlangObject fold(OtpErlangLong partition, 
                                byte[] bucket,
                                final OtpErlangPid streamToPid,
                                MDB.ForEachFunction forEachFunction) {
        log.info("fold: " + partition + " -> " + streamToPid);
        try {
            if (bucket != null) {
                mdb.foldBucket(partition.toString(), 
                                  bucket,
                                  forEachFunction);
            } else {
                mdb.fold(partition.toString(), 
                               forEachFunction);
            }
            getMbox().send(streamToPid, new OtpErlangAtom("done"));
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
        }
        return null;
    }
    
    /*
     * fold_buckets is currently a cast (no expected reply); results are sent directly to
     *  streamToPid.
    */
    public OtpErlangObject fold_buckets(OtpErlangLong partition, final OtpErlangPid streamToPid) {
        log.info("fold_buckets: " + partition + " -> " + streamToPid);
        try {
            mdb.foldBucketNames(partition.toString(), new MDB.ForEachFunction() {
                public boolean each(byte[] bucket, byte[] _key, byte[] _value) {
                    getMbox().send(streamToPid, new OtpErlangBinary(bucket));
                    return true;
                }
            });
            getMbox().send(streamToPid, new OtpErlangAtom("done"));
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
        }
        return null;
    }

    /*
     * fold_objects is currently a cast (no expected reply); results are sent directly to
     *  streamToPid.
    */
    public OtpErlangObject fold_objects(OtpErlangLong partition, 
                                        final OtpErlangPid streamToPid) {
        return fold_objects(partition, null, streamToPid);
    }
    
    public OtpErlangObject fold_objects(OtpErlangLong partition, 
                                        OtpErlangBinary bucket,
                                        final OtpErlangPid streamToPid) {
        log.info("fold_objects: " + partition + " -> " + streamToPid);
        
        try {
            final String rpcNode = Mecha.getConfig().getString("riak-nodename");

            String temporaryOtpNodename = 
                HashUtils.sha1(
                    rpcNode + 
                    "-" + 
                    System.currentTimeMillis()) + 
                "@127.0.0.1";
            
            final OtpProcessManager foldProcMgr = 
                new OtpProcessManager(temporaryOtpNodename, 
                                      Mecha.getConfig().getString("riak-cookie"));
            final OtpErlangObject[] args1 = 
                { new OtpErlangAtom("message_queue_len") };
            final OtpErlangObject[] args = 
                { streamToPid, new OtpErlangList( args1 ) };
            final AtomicInteger tcount = 
                new AtomicInteger(0);
            final OtpRPC otpRPC = 
                new OtpRPC(foldProcMgr.getNode(), rpcNode);
            
            try {
                if (null != bucket) {
                    mdb.foldBucket(partition.toString(), 
                                bucket.binaryValue(),
                                new MDB.ForEachFunction() {
                        public boolean each(byte[] bucket, byte[] key, byte[] value) {
                            OtpErlangObject[] bkv = 
                                { new OtpErlangBinary(bucket), 
                                  new OtpErlangBinary(key), 
                                  new OtpErlangBinary(value)
                                };
                            OtpErlangTuple otpbkv = new OtpErlangTuple(bkv);
                            getMbox().send(streamToPid, otpbkv);
                            return true;
                        }
                    });
                } else {
                    mdb.fold(partition.toString(), new MDB.ForEachFunction() {
                        public boolean each(byte[] bucket, byte[] key, byte[] value) {
                            try {
                                OtpErlangObject[] bkv = 
                                    { new OtpErlangBinary(bucket), 
                                      new OtpErlangBinary(key), 
                                      new OtpErlangBinary(value)
                                    };
                                OtpErlangTuple otpbkv = new OtpErlangTuple(bkv);
                                getMbox().send(streamToPid, otpbkv);
                                
                                /*
                                 * TODO: Either configurable backpressure ACKs or 
                                 *       bite the bullet & enforce synchronous replies.
                                */
                                if (tcount.incrementAndGet()>=5000) {
                                    tcount.set(0);
                                    while(true) {
                                        log.info("fold: " + streamToPid + ": requesting mailbox size...");
                                        OtpErlangList obj = (OtpErlangList) otpRPC.rpc("erlang", "process_info", args);
                                        
                                        // [{message_queue_len,851520}]
                                        OtpErlangObject[] tuples1 = obj.elements();
                                        OtpErlangTuple boxSizeTuple = (OtpErlangTuple) tuples1[0];
                                        long mbox_sz = ((OtpErlangLong)boxSizeTuple.elementAt(1)).longValue();
                                        if (mbox_sz > 5000) {
                                            log.info("fold: " + streamToPid + ": mailbox size: " + mbox_sz + " (sleeping)");
                                            Thread.sleep(1000);
                                            continue;
                                        } else {
                                            log.info("fold: " + streamToPid + ": mailbox size: " + mbox_sz + ": ok!");
                                            break;
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                Mecha.getMonitoring().error("mecha.riak-connector", ex);
                                ex.printStackTrace();
                            }
                            return true;
                        }
                    });
                }
            } catch (Exception ex1) {
                Mecha.getMonitoring().error("mecha.riak-connector", ex1);
                ex1.printStackTrace();
                
            } finally {
                foldProcMgr.shutdown();
                getMbox().send(streamToPid, new OtpErlangAtom("done"));
            }
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
        }
        return null;
    }
    
    /*
     * fold_keys is currently a cast (no expected reply); results are sent directly to
     *  streamToPid.
    */
    public OtpErlangObject fold_keys(OtpErlangLong partition, 
                                     final OtpErlangPid streamToPid) {
        return fold_keys(partition, null, streamToPid);
    }
    
    public OtpErlangObject fold_keys(OtpErlangLong partition, 
                                     OtpErlangBinary bucket,
                                     final OtpErlangPid streamToPid) {
        log.info("fold_keys: " + partition + " -> " + streamToPid);
        try {
            if (null != bucket) {
                mdb.foldBucket(partition.toString(), 
                            bucket.binaryValue(),
                            new MDB.ForEachFunction() {
                    public boolean each(byte[] bucket, byte[] key, byte[] value) {
                        OtpErlangObject[] bkv = 
                            { new OtpErlangBinary(bucket), 
                              new OtpErlangBinary(key)
                            };
                        OtpErlangTuple otpbkv = new OtpErlangTuple(bkv);
                        getMbox().send(streamToPid, otpbkv);
                        return true;
                    }
                });
            } else {
                mdb.fold(partition.toString(), new MDB.ForEachFunction() {
                    public boolean each(byte[] bucket, byte[] key, byte[] value) {
                        OtpErlangObject[] bkv = 
                            { new OtpErlangBinary(bucket), 
                              new OtpErlangBinary(key)
                            };
                        OtpErlangTuple otpbkv = new OtpErlangTuple(bkv);
                        getMbox().send(streamToPid, otpbkv);
                        return true;
                    }
                });
            }
            getMbox().send(streamToPid, new OtpErlangAtom("done"));
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
        }
        return null;
    }
    
    public OtpErlangObject is_empty(OtpErlangLong partition, OtpErlangAtom _placeholder) {
        try {
            if(mdb.isEmpty(partition.toString())) return new OtpErlangBoolean(true);
            return new OtpErlangBoolean(false);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    public OtpErlangObject drop(OtpErlangLong partition, OtpErlangAtom _placeholder) {
        try {
            mdb.drop(partition.toString());
            return new OtpErlangAtom("ok");
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    //
    // TODO: diffuse request to all bucket db's under partition & gather
    //
    
    //
    // TODO: reduce redundant code between the two list variations
    //
    
    public OtpErlangObject list(OtpErlangLong partition, OtpErlangAtom _placeholder) {
        log.info("list: " + partition);
        try {
            List<OtpErlangTuple> otpKeyList = 
                new ArrayList<OtpErlangTuple>();
            List<byte[][]> keys = mdb.listKeys(partition.toString());
            if (keys == null) {
                final List<byte[][]> keys1 = new ArrayList<byte[][]>();
                mdb.fold(partition.toString(), new MDB.ForEachFunction() {
                    public boolean each(byte[] bucket, byte[] key, byte[] val) {
                        byte[][] bkey = {bucket, key};
                        keys1.add(bkey);
                        return true;
                    }
                });
                keys = keys1;
            }
            for(byte[][] bkey0: keys) {
                OtpErlangObject[] bkey = new OtpErlangObject[2];
                bkey[0] = new OtpErlangBinary(bkey0[0]);
                bkey[1] = new OtpErlangBinary(bkey0[1]);
                OtpErlangTuple bkey1 = new OtpErlangTuple(bkey);
                otpKeyList.add(bkey1);
            }
            OtpErlangObject[] otpKeys = keys.toArray(new OtpErlangTuple[0]);
            return new OtpErlangList(otpKeys);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
    //
    // TODO: reduce redundant code between the two list variations
    //
    
    public OtpErlangObject list(OtpErlangLong partition, OtpErlangBinary bucket) {
        log.info("list: " + partition + ": " + (new String(bucket.binaryValue())));
        try {
            List<OtpErlangTuple> otpKeyList = 
                new ArrayList<OtpErlangTuple>();
            List<byte[]> keys = mdb.listKeys(partition.toString(), bucket.binaryValue());
            if (keys == null) {
                final List<byte[]> keys1 = new ArrayList<byte[]>();
                mdb.foldBucket(partition.toString(), 
                               bucket.binaryValue(), 
                               new MDB.ForEachFunction() {
                    public boolean each(byte[] bucket, byte[] key, byte[] val) {
                        keys1.add(key);
                        return true;
                    }
                });
                keys = keys1;
            }
            for(byte[] key: keys) {
                OtpErlangObject[] bkey = new OtpErlangObject[2];
                bkey[0] = bucket;
                bkey[1] = new OtpErlangBinary(key);
                OtpErlangTuple bkey1 = new OtpErlangTuple(bkey);
                otpKeyList.add(bkey1);
            }
            OtpErlangObject[] otpKeys = keys.toArray(new OtpErlangTuple[0]);
            return new OtpErlangList(otpKeys);
        } catch (Exception ex) {
            Mecha.getMonitoring().error("mecha.riak-connector", ex);
            ex.printStackTrace();
            return new OtpErlangAtom("error");
        }
    }
    
}
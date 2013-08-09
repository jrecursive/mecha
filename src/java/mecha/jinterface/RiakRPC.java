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
 * 
 * (C) 2013 John Muellerleile @jrecursive
 *
 */

package mecha.jinterface;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.locks.ReentrantLock;

import com.ericsson.otp.erlang.*;
import mecha.json.*;

import mecha.Mecha;
import mecha.util.*;

public class RiakRPC {
    final private static Logger log = 
        Logger.getLogger(RiakRPC.class.getName());

    private OtpRPC otpRPC;
    final private ReentrantLock rpcLock = 
        new ReentrantLock();
    private OtpProcessManager otpMgr;

    public RiakRPC() throws Exception {
        otpMgr = null;
        otpRPC = getOtpRPC();
    }
    
    private OtpRPC getOtpRPC() throws Exception {
        final String rpcNode = Mecha.getConfig().getString("riak-nodename");
        
        final String temporaryOtpNodename = 
            HashUtils.sha1(
                rpcNode + 
                "-" + 
                UUID.randomUUID()) + 
            "@127.0.0.1";
        
        while(true) {
            try {
                if (otpMgr != null) {
                    log.info("killing existing otp process manager...");
                    otpMgr.shutdown();
                }
                otpMgr = 
                    new OtpProcessManager(temporaryOtpNodename, 
                                          Mecha.getConfig().getString("riak-cookie"));
                OtpRPC otpRPC = 
                    new OtpRPC(otpMgr.getNode(), rpcNode);
                return otpRPC;
            } catch (java.io.IOException ex) {
                try {
                    Mecha.getMonitoring().error("mecha.riak-connector", ex);
                } catch (java.lang.NullPointerException npe) {
                    /*
                     * swallow this exception because if mecha tries to
                     *  talk to riak before monitoring and logging has
                     *  been brought up when it is first launched.  here
                     *  we want to just retry quietly until riak is available.
                    */
                }
                log.info("Cannot contact riak, retrying...");
                Thread.sleep(1000);
            }
        }
    }
    
    /*
     * ALWAYS go through this method for erlang RPC.  Locking is essential!
    */
    public OtpErlangObject rpc(String module,
                               String fun,
                               OtpErlangObject[] args) throws Exception {
        OtpErlangObject obj = null;
        rpcLock.lock();
        try {
            while(true) {
                try {
                    obj = (OtpErlangObject)
                        otpRPC.rpc(module, fun, args);
                    return obj;
                } catch (java.io.IOException ex) {
                    if (Mecha.isShuttingDown()) {
                        log.info("Mecha shutting down, ignoring rpc failure.");
                    } else {
                        Mecha.getMonitoring().error("mecha.riak-connector", ex);
                        log.info("Riak link broken, restarting...");
                        Mecha.getMonitoring().log("mecha.jinterface.riak-rpc.rpc", 
                                                  "Riak link broken, restarting...");
                        Mecha.riakDown();
                        otpRPC = getOtpRPC();
                        log.info("Riak link restored.");
                        Mecha.getMonitoring().log("mecha.jinterface.riak-rpc.rpc", 
                                                  "Riak link restored.");
                    }
                }
            }
        } finally {
            rpcLock.unlock();
        }
    }
    
    /* 
     * get number of messages waiting in erlang mailbox for pid
    */
    public long getMboxSize(OtpErlangPid pid) throws Exception {
        final OtpErlangObject[] args0 = 
            { new OtpErlangAtom("message_queue_len") };
        final OtpErlangObject[] args1 = 
            { pid, new OtpErlangList( args0 ) };
        OtpErlangList obj = (OtpErlangList) rpc("erlang", "process_info", args1);
        
        // [{message_queue_len,851520}]
        OtpErlangObject[] tuples1 = obj.elements();
        OtpErlangTuple mailboxSizeTuple = (OtpErlangTuple) tuples1[0];
        long mboxSize = ((OtpErlangLong) mailboxSizeTuple.elementAt(1)).longValue();
        return mboxSize;
    }
    
    /*
     * compute coverage for bucket
     *
     * INFO: obj = [{45671926166590716193865151022383844364247891968,'riak@10.60.40.68'},
     *              {137015778499772148581595453067151533092743675904,'riak@10.60.40.78'}, ...
     *
    */
    public Map<String, Set<String>> getCoverage(String bucket) throws Exception {
        Map<String, Set<String>> coveragePlan =
            new HashMap<String, Set<String>>();
        OtpErlangObject[] args = { new OtpErlangBinary(bucket.getBytes()) };
        OtpErlangList obj = 
            (OtpErlangList) rpc("riak_kv_mecha_backend", "bucket_coverage", args);
        OtpErlangObject[] coverageTuples = obj.elements();
        for(OtpErlangObject tupObj: coverageTuples) {
            OtpErlangTuple coverageTuple = (OtpErlangTuple) tupObj;
            String partition = "" + coverageTuple.elementAt(0);
            String node = ((OtpErlangAtom) coverageTuple.elementAt(1)).atomValue();
            String host = node.split("@")[1];
            Set<String> partitions;
            if (null == coveragePlan.get(host)) {
                partitions = new HashSet<String>();
            } else {
                partitions = coveragePlan.get(host);
            }
            partitions.add(partition);
            coveragePlan.put(host, partitions);
        }
        return coveragePlan;
    }
    
/*
     * compute coverage for bucket
     *
     * INFO: obj = [{45671926166590716193865151022383844364247891968,'riak@10.60.40.68'},
     *              {137015778499772148581595453067151533092743675904,'riak@10.60.40.78'}, ...
     *
    */
    public Set<String> getClusterHosts() throws Exception {
        Set<String> hosts = new HashSet<String>();
        OtpErlangObject[] args = { };
        OtpErlangList obj = 
            (OtpErlangList) rpc("erlang", "nodes", args);
        OtpErlangObject[] hostAtoms = obj.elements();
        for(OtpErlangObject hostAtomObj: hostAtoms) {
            OtpErlangAtom hostAtom = (OtpErlangAtom) hostAtomObj;
            hosts.add(hostAtom.atomValue().split("@")[1]);
        }
        hosts.add(Mecha.getConfig().getJSONObject("riak-config").getString("pb-ip"));
        return hosts;
    }
    
    /*
     * compute preflist for a bucket, key pair.
     *
     * [{{548063113999088594326381812268606132370974703616,
     *    'riak@75.125.240.202'},
     *   primary},
     *  {{593735040165679310520246963290989976735222595584,
     *    'riak@75.125.240.202'},
     *   primary},
     *  {{639406966332270026714112114313373821099470487552,
     *    'riak@75.125.240.202'},
     *   primary}]
     * 
     * returns a map of host -> set[ partition, partition, ... ]
     * 
    */
    
    public Map<String, Set<String>> 
        getBKeyPreflist(String bucket, String key) throws Exception {
        Map<String, Set<String>> preflist =
            new HashMap<String, Set<String>>();
        OtpErlangObject[] args = { new OtpErlangBinary(bucket.getBytes()),
                                   new OtpErlangBinary(key.getBytes()) };
        OtpErlangList obj = 
            (OtpErlangList) rpc("riak_kv_mecha_backend", "bkey_preflist", args);
        OtpErlangObject[] preflistTuples = obj.elements();
        for(OtpErlangObject tupObj: preflistTuples) {
            OtpErlangTuple preflistTuple1 = (OtpErlangTuple) tupObj;
            OtpErlangTuple preflistTuple = 
                (OtpErlangTuple) preflistTuple1.elementAt(0);
            String partition = "" + preflistTuple.elementAt(0);
            String node = ((OtpErlangAtom) preflistTuple.elementAt(1)).atomValue();
            String host = node.split("@")[1];
            Set<String> partitions;
            if (null == preflist.get(host)) {
                partitions = new HashSet<String>();
            } else {
                partitions = preflist.get(host);
            }
            partitions.add(partition);
            preflist.put(host, partitions);
        }
        return preflist;
    }
    
    /*
     * Shutdown the erlang vm.
    */
    
    public void shutdown() throws Exception {
        OtpErlangObject[] args = { };
        OtpErlangObject obj = rpc("init", "stop", args);
        log.info("Riak shutdown: " + obj.toString());
    }
    
    /*
     * application:get_env(atom, atom)
     *
     * (riak@127.0.0.1)7> application:get_env(riak_core, http).
     * {ok,[{"0.0.0.0",18098}]}
     *
    */
    
    public OtpErlangObject getApplicationEnv(String app, String par) throws Exception {
        
        OtpErlangObject[] args = { new OtpErlangAtom(app), new OtpErlangAtom(par) };
        OtpErlangObject result = rpc("application", "get_env", args);
        
        /*
         * atom 'undefined' returned
        */
        if (result instanceof OtpErlangAtom) {
            if (((OtpErlangAtom) result).atomValue().equals("undefined")) {
                return null;
            }
        
        /*
         * a result of the form {ok, <term>} returned
        */
        } else if (result instanceof OtpErlangTuple) {
            OtpErlangTuple t = (OtpErlangTuple) result;
            String status = ((OtpErlangAtom) t.elementAt(0)).atomValue();
            if (status.equals("ok")) {
                return t.elementAt(1);
            }
        }
        
        /*
         * a result of any other form is passed through
         *  untouched.
        */
        return result;
    }
    
    /*
     * introspect built in web server host & port
    */
    public String getLocalRiakURL() throws Exception {
        OtpErlangList result = 
            (OtpErlangList) getApplicationEnv("riak_core", "http");
        OtpErlangTuple t = (OtpErlangTuple) result.elementAt(0);
        String host = ((OtpErlangString) t.elementAt(0)).stringValue();
        String port = "" + t.elementAt(1);
        return "http://" + host + ":" + port;
    }
    
    /* 
     * introspect protobuffers IP address
    */
    public String getLocalPBIP() throws Exception {
        return ((OtpErlangString)getApplicationEnv("riak_kv", "pb_ip")).stringValue();
    }
    
    /*
     * introspect protobuffers port
    */
    public int getLocalPBPort() throws Exception {
        return Integer.parseInt("" + getApplicationEnv("riak_kv", "pb_port"));
    }

}









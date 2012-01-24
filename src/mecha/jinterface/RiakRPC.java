package mecha.jinterface;

import java.util.*;
import java.util.logging.*;
import java.util.concurrent.locks.ReentrantLock;

import com.ericsson.otp.erlang.*;
import org.json.*;

import mecha.Mecha;
import mecha.util.*;

public class RiakRPC {

    final private OtpRPC otpRPC;
    final private ReentrantLock rpcLock = 
        new ReentrantLock();

    public RiakRPC() throws Exception {
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
        
        final OtpProcessManager otpMgr = 
            new OtpProcessManager(temporaryOtpNodename, 
                                  Mecha.getConfig().getString("riak-cookie"));
        final OtpRPC otpRPC = 
            new OtpRPC(otpMgr.getNode(), rpcNode);
        
        return otpRPC;
    }
    
    /*
     * ALWAYS go through this method for erlang RPC.  Locking is essential!
    */
    public OtpErlangObject rpc(String module,
                               String fun,
                               OtpErlangObject[] args) throws Exception {
        rpcLock.lock();
        try {
            OtpErlangObject obj = (OtpErlangObject)
                otpRPC.rpc(module, fun, args);
            return obj;
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
            (OtpErlangList) rpc("riak_kv_fabric_backend", "bucket_coverage", args);
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
        return "http://" + host + ":" + port + "/riak/";
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









package mecha.jinterface;

import java.util.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;
import org.jetlang.channels.*;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;

public class OtpProcessManager {
    final private ConcurrentHashMap<OtpErlangPid,OtpProcess> processTable;
    
    /*
     * rpcCache key is the name@host of the remote node.
    */
    final private ConcurrentHashMap<String,OtpRPC> rpcCache;
    
    final private OtpNode otpNode;
    
    final private ExecutorService otpProcessExecutor;
    final private PoolFiberFactory otpProcessFiberFactory;

    public OtpProcessManager(String nodeName, String cookie) throws java.io.IOException {
        this(new OtpNode(nodeName, cookie));
    }
    
    public OtpProcessManager(OtpNode otpNode) throws java.io.IOException {
        this.otpNode = otpNode;
        processTable = new ConcurrentHashMap<OtpErlangPid, OtpProcess>();
        rpcCache = new ConcurrentHashMap<String, OtpRPC>();
        otpProcessExecutor = Executors.newFixedThreadPool((Runtime.getRuntime()).availableProcessors());
        otpProcessFiberFactory = new PoolFiberFactory(otpProcessExecutor);
    }
    
    // shutdown
    public void shutdown() {
        synchronized(this) {
            for(OtpProcess process: processTable.values()) {
                process.kill();
            }
            otpProcessFiberFactory.dispose();
            otpProcessExecutor.shutdown();
        }
    }
    
    // getOtpNode
    
    public OtpNode getNode() {
        return otpNode;
    }
    
    // getOtpProcess
    
    public OtpProcess getProcess(OtpErlangPid pid) {
        return processTable.get(pid);
    }
    
    public OtpProcess getProcess(String processName) {
        OtpErlangPid pid = otpNode.whereis(processName);
        if (pid == null) return null;
        return processTable.get(pid);
    }
    
    // spawn
    
    public OtpErlangPid spawn(OtpProcess process) throws NamedProcessExistsException {
        return spawn(null, process);
    }
    
    public OtpErlangPid spawn(String name, OtpProcess process) throws NamedProcessExistsException {
        OtpMbox mbox;
        OtpErlangPid pid;
        if (name != null) {
            if (otpNode.whereis(name) != null) {
                throw new NamedProcessExistsException();
            }
            process.setName(name);
            mbox = otpNode.createMbox(name);
        } else {
            mbox = otpNode.createMbox();
        }
        pid = mbox.self();
        process.setMbox(mbox);
        process.setPid(pid);
        
        Fiber fiber = otpProcessFiberFactory.create();
        fiber.start();
        Channel<OtpMsg> processChannel = new MemoryChannel<OtpMsg>();
        processChannel.subscribe(fiber, process);
        process.setChannel(processChannel);
        process.setFiber(fiber);
        process.startReceive();
        process.setOwner(this);
        processTable.put(pid, process);
        return pid;
    }
    
    // rpc
    
    public OtpErlangObject rpc(OtpNode remoteNode,
                               String mod,
                               String fun,
                               OtpErlangObject[] args) throws java.io.IOException, 
                                                              com.ericsson.otp.erlang.OtpAuthException,
                                                              com.ericsson.otp.erlang.OtpErlangExit {
        return rpc(remoteNode.node(), mod, fun, args);
    }
    
    public OtpErlangObject rpc(OtpNode remoteNode,
                               String mod,
                               String fun,
                               OtpErlangList args) throws java.io.IOException, 
                                                          com.ericsson.otp.erlang.OtpAuthException,
                                                          com.ericsson.otp.erlang.OtpErlangExit {
        return rpc(remoteNode.node(), mod, fun, args.elements());
    }
    
    public OtpErlangObject rpc(String remoteNode,
                               String mod,
                               String fun,
                               OtpErlangList args) throws java.io.IOException, 
                                                          com.ericsson.otp.erlang.OtpAuthException,
                                                          com.ericsson.otp.erlang.OtpErlangExit {
        return rpc(remoteNode, mod, fun, args.elements());
    }
    
    public OtpErlangObject rpc(String remoteNode,
                               String mod,
                               String fun,
                               OtpErlangObject[] args) throws java.io.IOException, 
                                                              com.ericsson.otp.erlang.OtpAuthException,
                                                              com.ericsson.otp.erlang.OtpErlangExit {
        OtpRPC otpRPC = rpcCache.get(remoteNode);
        if (otpRPC == null) {
            otpRPC = new OtpRPC(getNode(), remoteNode);
            rpcCache.put(remoteNode, otpRPC);
        }
        return otpRPC.rpc(mod, fun, args);
    }
}












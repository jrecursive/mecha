package mecha.jinterface;

import java.util.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;

public class OtpRPC {
    final private OtpSelf otpSelf;
    final private OtpPeer otpPeer;
    final private OtpConnection otpConnection;
    
    protected OtpRPC(OtpNode localNode, String remoteNode) throws java.io.IOException, 
                                                                  com.ericsson.otp.erlang.OtpAuthException {
        otpSelf = new OtpSelf(localNode.alive() + "_rpc@" + localNode.host(), localNode.cookie());
        otpPeer = new OtpPeer(remoteNode);
        otpPeer.setCookie(localNode.cookie());
        otpConnection = otpSelf.connect(otpPeer);
    }
    
    public OtpErlangObject rpc(String mod,
                               String fun,
                               OtpErlangObject[] args) throws java.io.IOException, 
                                                              com.ericsson.otp.erlang.OtpAuthException,
                                                              com.ericsson.otp.erlang.OtpErlangExit {
        OtpErlangObject result = null;
        synchronized(this) {
            otpConnection.sendRPC(mod, fun, args);
            result = otpConnection.receiveRPC();
        }
        return result;
    }
}

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
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Callback;
import org.jetlang.channels.*;
import org.jetlang.fibers.Fiber;

import mecha.Mecha;

public abstract class OtpProcess implements Callback<OtpMsg> {
    private String name = null;
    
    private OtpMbox mbox = null;
    private OtpErlangPid pid = null;
    
    private Fiber fiber = null;
    private Channel channel = null;
    
    private OtpProcessManager owner = null;
    
    private volatile boolean shutdown = false;
    private Thread receiveThread;
    final private Runnable receiveRunnable = new Runnable() {
        public void run() {
            try {
                while(!shutdown) {
                    try {
                        OtpMsg msg = mbox.receiveMsg(); // poll
                        cast(msg);
                    } catch (Exception innerException) {
                        Mecha.getMonitoring().error("mecha.jinterface", innerException);
                        innerException.printStackTrace();
                        // TODO: listener, notification
                    }
                }
            } catch (Exception outerException) {
                Mecha.getMonitoring().error("mecha.jinterface", outerException);
                outerException.printStackTrace();
                // TODO: listener, notification
            }
        }
    };
    
    protected void startReceive() {
        receiveThread = new Thread(receiveRunnable);
        receiveThread.start();
    }
    
    protected void setName(String name) {
        this.name = name;
    }
    
    protected void setMbox(OtpMbox mbox) {
        this.mbox = mbox;
    }
    
    protected void setPid(OtpErlangPid pid) {
        this.pid = pid;
    }
    
    protected void setFiber(Fiber fiber) {
        this.fiber = fiber;
    }
    
    protected void setChannel(Channel channel) {
        this.channel = channel;
    }
    
    protected void setOwner(OtpProcessManager owner) {
        this.owner = owner;
    }
    
    public String getName() {
        return name;
    }
    
    public OtpMbox getMbox() {
        return mbox;
    }
    
    public OtpErlangPid getPid() {
        return pid;
    }
    
    protected Channel getChannel() {
        return channel;
    }
    
    protected Fiber getFiber() {
        return fiber;
    }
    
    protected OtpProcessManager getOwner() {
        return owner;
    }
    
    // internal api
    
    protected void cast(OtpMsg msg) {
        channel.publish(msg);
    }
    
    protected void kill(String reason) {
        shutdown = true;
        fiber.dispose();
        mbox.exit(reason);
        // TODO: channel?
    }
    
    protected void kill() {
        kill("killed");
    }
    
    public void onMessage(OtpMsg msg) {
        receive(msg);
    }

    // public api
    
    abstract public void receive(OtpMsg msg);
}




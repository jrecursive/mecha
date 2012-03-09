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

package mecha.vm.channels;

import java.util.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;

/*
 * messaging channel
*/
public class PubChannel {
    final private static Logger log = 
        Logger.getLogger(PubChannel.class.getName());

    public String name;
    public Set<ChannelConsumer> members;
    
    public PubChannel(String name) {
        this.name = name;
        members = Collections.synchronizedSet(new HashSet());
    }
    
    public void addMember(ChannelConsumer cc) {
        members.add(cc);
    }
    
    public void removeMember(ChannelConsumer cc) {
        members.remove(cc);
        if (members.size() == 0) {
            Mecha.getChannels().destroyChannel(name);
        }
    }
    
    public void removeAllMembers() {
        members.clear();
        Mecha.getChannels().destroyChannel(name);
    }
    
    public Set<ChannelConsumer> getMembers() {
        return members;
    }
    
    public void send(String message) throws Exception {
        for(ChannelConsumer cc : members) {
            try {
                cc.onMessage(name, message);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    
    public void send(JSONObject message) throws Exception {
        for(ChannelConsumer cc : members) {
            cc.onMessage(name, message);
        }
    }

    public void send(byte[] message) throws Exception {
        for(ChannelConsumer cc : members) {
            cc.onMessage(name, message);
        }
    }
    
    public String getName() {
        return name;
    }

}

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

package mecha.client;

import java.lang.ref.*;
import mecha.json.*;
import mecha.client.net.*;

public abstract class MechaClientHandler {

    private WeakReference<TextClient> textClientRef = null;
    
    public void setTextClient(TextClient textClient) {
        textClientRef = new WeakReference<TextClient>(textClient);
    }
    
    public TextClient getTextClient() {
        return textClientRef.get();
    }
    
    public void onSystemMessage(JSONObject msg) throws Exception {
        System.out.println("<system> " + msg.toString(2));
    }
    
    public void onOpen() throws Exception {
        System.out.println("<connected>");
    }

    public void onClose() throws Exception {
        System.out.println("<disconnected>");
    }

    public void onError(Exception ex) {
        System.out.println("<error> " + ex.toString());
        ex.printStackTrace();
    }
    
    public abstract void onMessage(String msg);

    public void onDataMessage(String channel, JSONObject msg) throws Exception {
        System.out.println("<data: " + channel + "> " + msg.toString(2));
    }

    public void onDoneEvent(String channel, JSONObject msg) throws Exception {
        System.out.println("<done: " + channel + "> " + msg.toString(2));
    }
    
    public void onControlEvent(String channel, JSONObject msg) throws Exception {
        System.out.println("<control: " + channel + "> " + msg.toString(2));
    }
    
    public void onOk(String msg) throws Exception {
        System.out.println("<ok> " + msg);
    }
    
    public void onInfo(String msg) throws Exception {
        System.out.println("<info> " + msg);
    }
    
}

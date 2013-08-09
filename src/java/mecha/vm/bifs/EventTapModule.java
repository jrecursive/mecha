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

package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.io.*;
import java.net.*;

import mecha.Mecha;
import mecha.vm.channels.*;
import mecha.json.*;
import mecha.vm.*;

public class EventTapModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(EventTapModule.class.getName());

    public EventTapModule() throws Exception { super(); }
        
    public void moduleLoad() throws Exception { }
    
    public void moduleUnload() throws Exception { }
    
    public class Tap extends MVMFunction {
        final private String type;
        final private String filter;
        final private String channel;
        
        public Tap(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
            type = config.getString("type");
            filter = config.getString("filter");
            if (config.has("channel")) {
                channel = config.getString("channel");
            } else {
                channel = getContext().getClient().getChannel().getName();
            }
        }
        
        public void start(JSONObject msg) throws Exception {
            Mecha.getCEP().addQuery(type, filter, channel);
            //broadcastDone();
        }
    }
}
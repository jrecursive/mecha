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

package mecha.vm.bifs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.logging.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.vm.*;
import mecha.util.*;

import com.basho.riak.client.*;
import com.basho.riak.client.bucket.*;
import com.basho.riak.client.response.*;

public class RiakClientModule extends MVMModule {
    final private static Logger log = 
        Logger.getLogger(RiakClientModule.class.getName());
    
    IRiakClient riakClient;
    
    public RiakClientModule() throws Exception {
        super();
        final String riakHost = Mecha.getConfig().<String>get("server-addr");
        final int riakPort = Mecha.getConfig().getInt("riak-protobuf-port");
        riakClient = RiakFactory.pbcClient(riakHost, riakPort);
    }
    
    public void moduleLoad() throws Exception {
        ensureRiakClient();
    }
    
    public void moduleUnload() throws Exception {
        log.info("moduleUnload()");
    }
    
    private void ensureRiakClient() throws Exception {
        while (true) {
            try {
                riakClient.ping();
                return;
            } catch (RiakException ex) {
                if (Mecha.riakDown.get()) {
                    log.info("* riakDown flag true, waiting 1 second for riak link reconnection");
                    Thread.sleep(1000);
                }
                final String riakHost = Mecha.getConfig().<String>get("server-addr");
                final int riakPort = Mecha.getConfig().getInt("riak-protobuf-port");
                riakClient = RiakFactory.pbcClient(riakHost, riakPort);
            }
        }
    }
    
    public class Get extends MVMFunction {
        public Get(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            // TODO: decouple riak availability from riak client operational level
            // (or provide a different client that sets up long standing connections?)
            ensureRiakClient();
            final String bucketName = getConfig().<String>get("bucket");
            final String key = getConfig().<String>get("key");
            final Bucket bucket = riakClient.createBucket(bucketName).execute();
            final IRiakObject obj = bucket.fetch(key).execute();
            if (obj != null) {
               broadcastDataMessage(new JSONObject(obj.getValueAsString()));
            }
            broadcastDone();
        }
    }
    
    public class Put extends MVMFunction {
            
        public Put(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            // TODO: decouple riak availability from riak client operational level
            // (or provide a different client that sets up long standing connections?)
            ensureRiakClient();
            final String bucketName = getConfig().<String>get("bucket");
            log.info("bucketName = '" + bucketName + "'");
            final String key = getConfig().<String>get("key");
            log.info("key = '" + key + "'");
            final JSONObject putObj = getConfig().getJSONObject("object");
            log.info("putObj = " + putObj.toString(2));
            final Bucket bucket = riakClient.createBucket(bucketName).execute();
            bucket.store(key, putObj.toString()).execute();
            broadcastDone();
        }
    }
    
    public class Delete extends MVMFunction {
        public Delete(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            // TODO: decouple riak availability from riak client operational level
            // (or provide a different client that sets up long standing connections?)
            ensureRiakClient();
            final String bucketName = getConfig().<String>get("bucket");
            final String key = getConfig().<String>get("key");
            final Bucket bucket = riakClient.createBucket(bucketName).execute();
            bucket.delete(key).execute();
            broadcastDone();
        }
    }
    
    
    public class BucketProps extends MVMFunction {
        public BucketProps(String refId, MVMContext ctx, JSONObject config) throws Exception {
            super(refId, ctx, config);
        }
        
        public void onStartEvent(JSONObject msg) throws Exception {
            final String riakHost = Mecha.getConfig().<String>get("server-addr");
            final int riakPort = Mecha.getConfig().getInt("riak-http-port");
            final String bucketName = getConfig().<String>get("bucket");
            JSONObject props = 
                new JSONObject(HTTPUtils.fetch(
                    "http://" + 
                    riakHost + ":" + 
                    riakPort + "/riak/" +
                    URLEncoder.encode(bucketName)));
            broadcastDataMessage(props);
            broadcastDone();
        }
    }

    
}
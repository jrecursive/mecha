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

package mecha.json;

import java.util.*;
import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public class JSONObject {    
    private ObjectMapper mapper = new ObjectMapper();
    private Map<String, Object> obj;
        
    public JSONObject() {
        obj = new LinkedHashMap<String, Object>();
    }
    
    public JSONObject(String json) throws Exception {
        obj = mapper.readValue(json, Map.class);
    }
    
    public JSONObject(Map<String, Object> obj) throws Exception {
        this.obj = obj;
    }
    
    /*
     * get
    */
    
    public <T> T get(String field) throws Exception {
        return (T) obj.get(field);
    }
    
    public int getInt(String field) throws Exception {
        return this.<Integer>get(field);
    }
    
    public String getString(String field) throws Exception {
        return this.<String>get(field);
    }
    
    public JSONArray getJSONArray(String field) throws Exception {
        Object jsonArrayObj = get(field);
        if (jsonArrayObj instanceof ArrayList) {
            return new JSONArray((ArrayList<Object>) jsonArrayObj);
        } else {
            return (JSONArray) jsonArrayObj;
        }
    }
    
    public JSONObject getJSONObject(String field) throws Exception {
        Object jsonObj = get(field);
        if (jsonObj instanceof Map) {
            return new JSONObject((Map<String, Object>) jsonObj);
        } else {
            return (JSONObject) jsonObj;
        }
    }
    
    /*
     * put
    */
    
    public void put(String field, Object value) throws Exception {
        if (value instanceof JSONObject) {
            putJSONObject(field, (JSONObject) value);
        } else if (value instanceof JSONArray) {
            putJSONArray(field, (JSONArray) value);
        } else {
            obj.put(field, value);
        }
    }
    
    public void put(String field, long value) throws Exception {
        put(field, new Long(value));
    }
    
    public void putJSONObject(String field, JSONObject jsonObj) throws Exception {
        obj.put(field, jsonObj._map());
    }

    public void putJSONArray(String field, JSONArray jsonArrayObj) throws Exception {
        obj.put(field, jsonArrayObj._list());
    }
    
    /*
     * misc
    */
    
    public void remove(String field) throws Exception {
        obj.remove(field);
    }
    
    public boolean has(String field) throws Exception {
        return obj.containsKey(field);
    }
    
    public static Set<String> getNames(JSONObject jsonObj) throws Exception {
        return jsonObj.getKeys();
    }
    
    public Set<String> getKeys() throws Exception {
        return obj.keySet();
    }
    
    protected Map<String, Object> _map() {
        return obj;
    }
    
    /*
     * toString
    */
    
     public String toString() {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
    
    public byte[] toBytes() throws Exception {
        return mapper.writeValueAsBytes(obj);
    }

    public String toString(int indent) throws Exception {
        StringWriter strWriter = new StringWriter();
        ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();
        return objectWriter.writeValueAsString(obj);
    }
    
}
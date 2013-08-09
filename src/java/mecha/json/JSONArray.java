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

package mecha.json;

import java.util.*;
import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public class JSONArray {
    
    private ObjectMapper mapper = new ObjectMapper();
    private List<Object> obj;
    
    public JSONArray() throws Exception {
        obj = new ArrayList<Object>();
    }
    
    public JSONArray(String json) throws Exception {
        obj = mapper.readValue(json, List.class);
    }
    
    public JSONArray(List<Object> obj) throws Exception {
        this.obj = obj;
    }
    
    /*
     * get
    */
    
    public <T> T get(int n) throws Exception {
        return (T) obj.get(n);
    }
    
    public String getString(int n) throws Exception {
        return this.<String>get(n);
    }
    
    public JSONObject getJSONObject(int n) throws Exception {
        Object jsonObj = get(n);
        if (jsonObj instanceof Map) {
            return new JSONObject((Map<String, Object>) jsonObj);
        } else {
            return (JSONObject) jsonObj;
        }
    }   
    
    /*
     * put
    */
    
    public void put(Object value) throws Exception {
        obj.add(value);
    }
    
    public void put(JSONObject jsonObj) {
        obj.add(jsonObj._map());
    }
    
    public void put(JSONArray jsonArrayObj) {
        obj.add(jsonArrayObj._list());
    }
    
    /*
     * misc
    */
    
    public int length() {
        return obj.size();
    }
    
    protected List<Object> _list() {
        return obj;
    }
    
    public List<Object> asList() {
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
}
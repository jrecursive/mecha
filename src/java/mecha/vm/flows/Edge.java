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

package mecha.vm.flows;

import java.util.*;
import java.util.logging.*;
import org.jgrapht.graph.DefaultWeightedEdge;
import mecha.json.*;

public class Edge<Vertex> extends DefaultWeightedEdge {
    final private static Logger log = 
        Logger.getLogger(Edge.class.getName());
    
    final private Vertex source;
    final private Vertex target;
    
    /*
     * rel: "relation" or "label"
    */
    final private String rel;
    
    /*
     * data payload
    */
    final private JSONObject data;
	
    public Edge(String refId, 
                Vertex source, 
                Vertex target, 
                String rel, 
                JSONObject expr) {
        this.source = source;
        this.target = target;
        this.rel = rel;
        data = new JSONObject();
        try {
            data.put(Flow.REF_ID, refId);
            data.put(Flow.EXPR, expr);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public Vertex getSource() {
        return source;
    }
    
    public Vertex getTarget() {
        return target;
    }
    
	/*
	 * data put, get
    */
	
	public void put(String k, Object v) throws Exception {
		data.put(k,v);
	}
	
	public <T> T get(String k) throws Exception {
        return (T) data.<T>get(k);
    }
	
    /*
     * helpers
    */
    
    public String getRefId() throws Exception {
        return this.<String>get(Flow.REF_ID);
    }
    
    public JSONObject getExpr() throws Exception {
        return new JSONObject(this.<Map>get(Flow.EXPR));
    }
    
    public String getRel() {
        return rel;
    }
	
	/*
	 * pretty print
    */
	
	public String toString(int d) throws Exception {
		return data.toString(4).replaceAll("\"", "\\\"");
	}
    
    public String toString() {
        return rel;
    }
    
}

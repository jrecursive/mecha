package mecha.vm.parser;

import java.util.logging.*;
import org.jgrapht.graph.DefaultEdge;
import org.json.*;

public class QueryEdge<V> extends DefaultEdge {
    final private static Logger log = 
        Logger.getLogger(QueryEdge.class.getName());
    
    private V v1;
    private V v2;
    private String label;
    public JSONObject data;
	
    public QueryEdge(V v1, V v2, String label) {
        this.v1 = v1;
        this.v2 = v2;
        this.label = label;
        data = new JSONObject();
    }

    public V getV1() {
        return v1;
    }

    public V getV2() {
        return v2;
    }

    public String toString() {
        return label;
    }

	public QueryEdge() {
		data = new JSONObject();
	}
	
	public void put(String k, String v) throws Exception {
		data.put(k,v);
	}
	
	public String get(String k) throws Exception {
		return data.getString(k);
	}
	
	public String toString(int d) throws org.json.JSONException {
		return data.toString(4).replaceAll("\"", "\\\"");
	}

}

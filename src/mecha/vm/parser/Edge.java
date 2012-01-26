package mecha.vm.parser;

import java.util.logging.*;
import org.jgrapht.graph.DefaultEdge;
import mecha.json.*;

public class Edge<V> extends DefaultEdge {
    final private static Logger log = 
        Logger.getLogger(Edge.class.getName());
    
    private V v1;
    private V v2;
    private String label;
    public JSONObject data;
	
    public Edge(V v1, V v2, String label) {
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

	public Edge() {
		data = new JSONObject();
	}
	
	public void put(String k, String v) throws Exception {
		data.put(k,v);
	}
	
	public <T> T get(String k) throws Exception {
        return (T) data.get(k);
    }
	
	public String toString(int d) throws Exception {
		return data.toString(4).replaceAll("\"", "\\\"");
	}

}

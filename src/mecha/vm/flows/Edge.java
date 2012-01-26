package mecha.vm.flows;

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
        return this.<JSONObject>get(Flow.EXPR);
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

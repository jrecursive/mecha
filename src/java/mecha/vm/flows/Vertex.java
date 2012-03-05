package mecha.vm.flows;

import java.util.logging.*;
import mecha.json.*;

public class Vertex extends JSONObject {	
    final private static Logger log = 
        Logger.getLogger(Vertex.class.getName());
        
    /*
     * For sane and practical use of DOTExporter.
     * TODO: better scheme.. 
    */
    static int vertexDotIdCounter = 0;
    
    /*
     * For DOTExporter functionality.
    */
    public int vertexDotId;
	
	/*
	 * Vertex(GUID)
    */
	public Vertex(String refId, JSONObject expr) {
        super();
        vertexDotId = vertexDotIdCounter++;
        try {
    	   put(Flow.REF_ID, refId);
    	   put(Flow.EXPR, expr);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	/*
	 * put
    */
	
	public void put(String k, Object v) throws Exception {
		super.put(k,v);
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
	
	/*
	 * This mess is due to the .dot graph export format
	 *  (which I use constantly to debug & analyze
	 *  query plans).
	 * 
	 * TODO: cleaner export (move to DOTExporter 
	 *       plugin classes)
    */
	public String toString(int d) throws Exception {
		return super.toString(4).replaceAll("\"", "\\\"");
	}

	public String toString() {
		String str = "";
		try {
		    /*
		     * For sane and practical use of DOTExporter.
		    */
			str = super.toString();
			JSONObject jo = new JSONObject(str);
            str = jo.toString();
			while(str.indexOf("\"")!=-1) {
				str = str.replace("\"", "'");
			}
		} catch (Exception ex) {
            ex.printStackTrace();
		}
		log.info("str = " + str);
		return str;
	}
}



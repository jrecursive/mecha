package mecha.vm.parser;

import java.util.logging.*;
import org.json.*;

public class Vertex extends JSONObject {	
    final private static Logger log = 
        Logger.getLogger(Vertex.class.getName());
    
    /*
     * For sane and practical use of DOTExporter.
    */
    static int vertexDotIdCounter = 0;
    public int vertexDotId;
	
	public Vertex(String refId) {
        super();
        vertexDotId = vertexDotIdCounter++;
        try {
    	   super.put("ref_id", refId);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	public void put(String k, String v) throws Exception {
		super.put(k,v);
	}
	
	public String toString(int d) throws org.json.JSONException {
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



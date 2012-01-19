package mecha.vm.parser;

import java.util.logging.*;
import org.json.*;

public class Vertex extends JSONObject {	
    final private static Logger log = 
        Logger.getLogger(Vertex.class.getName());
    
    static int node_id = 0;
    public int this_node_id;
	
	public Vertex(String refId) {
        super();
        node_id++;
        this_node_id = node_id;
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



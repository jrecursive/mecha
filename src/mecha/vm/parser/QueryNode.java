package mecha.vm.parser;

import java.util.logging.*;
import org.json.*;

public class QueryNode extends JSONObject {	
    final private static Logger log = 
        Logger.getLogger(QueryNode.class.getName());
    
    static int node_id = 0;
    public int this_node_id;
	
	public QueryNode(String refId) {
        super();
        node_id++;
        this_node_id = node_id;
        try {
    	   super.put("ref_id", refId);
        } catch (Exception ex) {
            ex.printStackTrace();
            // ultra fail whale if we EVER get here
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
			str = super.toString();
			
			JSONObject jo = new JSONObject(str);
			
			// TODO: DOUBLE CHECK REF_ID ETC USE
            /*
            jo.remove("ref_id");
            jo.remove("query");
            jo.remove("node_type");
            */
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



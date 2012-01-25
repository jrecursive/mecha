package mecha.vm.parser;

import java.util.*;
import java.util.logging.*;
import org.json.*;

public class CommandParser1 {
    final private static Logger log = 
        Logger.getLogger(CommandParser1.class.getName());
    
    final private static String OPERATOR_FIELD = "$";
	
	public JSONObject parse(String query) throws Exception {
        query = query.trim();
        JSONObject qry = new JSONObject();
        return process(query, qry);
    }
    
    private JSONObject process(String q, JSONObject obj) throws Exception {
        boolean isStateField = true;
        boolean isStateValue = false;
        boolean isWithinPhrase = false;
        boolean backslash = false;
        
        String operator = "";
        String currentField = "";
        String currentValue = "";
        
        for(int i=0; i<q.length(); i++) {
            String ch = q.substring(i,i+1);
            
            if (ch.equals("(")) {
                String rest = q.substring(i + 1);
                
                int nestCount = 1;
                boolean inQuotes = false;
                backslash = false;
                StringBuffer subExpr = new StringBuffer();
                for(int j=0; j<rest.length(); j++) {
                    String ch0 = rest.substring(j,j+1);
                    if (ch0.equals("\\")) {
                        subExpr.append(ch0);
                        backslash = true;
                        continue;
                    }
                    if (ch0.equals("\"")) {
                        subExpr.append(ch0);
                        if (inQuotes) {
                            if (backslash) {
                                backslash = false;
                                continue;
                            } else {
                                inQuotes = false;
                            }
                        } else {
                            inQuotes = true;
                        }
                        continue;
                    }
                    if (inQuotes) {
                        subExpr.append(ch0);
                        continue;
                    }
                    if (ch0.equals("(")) nestCount++;
                    else if (ch0.equals(")")) nestCount--;
                    if (nestCount == 0) {
                        break;
                    }
                    subExpr.append(ch0);
                }
                
                if (currentField.equals("")) {
                    currentField = "$args";
                }
                objPut(obj, 
                       currentField, 
                       process(subExpr.toString(), new JSONObject()));
                currentField = "";
                currentValue = "";
                i += subExpr.toString().length() + 1;
                continue;
            }
            
            if (isStateField &&
                ch.equals(":")) {
                isStateField = false;
                isStateValue = true;
                continue;
            }
            
            if (isStateField) {
                if (ch.equals(" ") ||
                    i == (q.length()-1)) {
                    if (i == (q.length()-1)) {
                        currentField += ch;
                    }
                    operator = currentField.trim();
                    if (!operator.equals("")) {
                        currentField = "";
                        objPut(obj,
                               OPERATOR_FIELD,
                               operator);
                    }
                    continue;
                } else {
                    currentField += ch;
                    continue;
                }
                
            }
            
            if (isStateValue) {
                if (ch.equals("\\") &&
                    isWithinPhrase) {
                    backslash = true;
                    continue;
                }
                if (ch.equals("\"") &&
                    q.substring(i-1,i).equals(":")) {
                    isWithinPhrase = true;
                    continue;
                } else if (ch.equals("\"") &&
                           backslash) {
                    currentValue += ch;
                    backslash = false;
                    continue;
                } else if (ch.equals("\"") &&
                           !backslash) {
                    isWithinPhrase = false;
                } else if (ch.equals(" ") &&
                           isWithinPhrase) {
                    currentValue += ch;
                    continue;
                }
                if (ch.equals(" ") ||
                    i == (q.length()-1)) {
                    if (i == (q.length()-1)) {
                        if (!ch.equals("\"")) {
                            currentValue += ch;
                        }
                    }
                    if (!currentField.equals("")) {
                        objPut(obj,
                               currentField,
                               currentValue);
                    }
                    isStateValue = false;
                    isStateField = true;
                    currentField = "";
                    currentValue = "";
                    continue;
                }
                if (!ch.equals("\"")) {
                    currentValue += ch;
                }
            }
        }
        obj = postProcess(obj);
        return obj;
    }
    
    private JSONObject postProcess(JSONObject obj) throws Exception {
        JSONObject obj1 = new JSONObject();
        
        for(String f : JSONObject.getNames(obj)) {
            JSONArray ar = obj.getJSONArray(f);
            if (ar.length() == 1) {
                if (ar.get(0) instanceof JSONObject) {
                    JSONObject o = ar.getJSONObject(0);
                    if (JSONObject.getNames(o).length == 1 &&
                        o.has("$")) {
                        obj1.put(f, o.get("$"));
                    } else {
                        obj1.put(f, o);
                    }
                } else {
                    obj1.put(f, ar.get(0));
                }
            } else {
                obj1.put(f, ar);
            }
        }
        return obj1;
    }
        

    private void objPut(JSONObject obj,
                        String field,
                        Object value) throws Exception {
        JSONArray ar;
        if (obj.has(field)) {
            ar = obj.getJSONArray(field);
        } else {
            ar = new JSONArray();
            obj.put(field, ar);
        }
        ar.put(value);
    }

}





package mecha.cep;

import java.util.*;
import mecha.json.*;

public class CEvent {
    public String type;
    public JSONObject obj;
    
    public CEvent(String type, JSONObject obj) {
        this.type = type;
        this.obj = obj;
    }
    
    public String getType() {
        return type;
    }
    
    public JSONObject getObj() {
        return obj;
    }
}

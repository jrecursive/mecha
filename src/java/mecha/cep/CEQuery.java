package mecha.cep;

import java.util.*;
import mecha.json.*;

public class CEQuery {
    final private String queryStr;
    final private String outputChannel;
    
    public CEQuery(String queryStr, String outputChannel) {
        this.queryStr = queryStr;
        this.outputChannel = outputChannel;
    }
    
    public String getQuery() {
        return queryStr;
    }
    
    public String getOutputChannel() {
        return outputChannel;
    }
}
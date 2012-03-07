package mecha.monitoring;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.common.*;

import org.apache.commons.math.stat.descriptive.*;

import mecha.Mecha;
import mecha.json.*;
import mecha.util.*;

/*
 * An individual metric is constituted by a Metric class.
 *
 * It consists of a name, one or more double fields, and 
 *  within the rolling solr core "system" log, zero or 
 *  indexed tags.
 * 
 * A name is of the standard mecha monitoring form:
 *  <subsystem>.<name>[.<name>. (...)]
 * 
*/

public class Metric {
    final private static Logger log = 
        Logger.getLogger(Metric.class.getName());
    
    final private String name;
    final private int windowSize;
    
    final private DescriptiveStatistics stats;
    
    public Metric(String name,
                  int windowSize) throws Exception {
        this.name = name;
        this.windowSize = windowSize;
        
        stats = new DescriptiveStatistics();
        stats.setWindowSize(windowSize);
    }
    
    public DescriptiveStatistics getStats() {
        return stats;
    }
    
    public void addValue(double v) {
        stats.addValue(v);
    }
    
    public void clear() {
        stats.clear();
    }
    
    public JSONObject asJSON(int recentValueCount) throws Exception {
        return asJSON(recentValueCount, true);
    }
    
    public JSONObject asJSON(int recentValueCount, boolean summary) throws Exception {
        JSONObject obj = new JSONObject();
        JSONArray values = new JSONArray();
        long sz = stats.getN();
        for(int i=0; i<recentValueCount; i++) {
            int idx = (int) sz - i - 1;
            if (idx < 0) break;
            values.put(stats.getElement(idx));
        }
        /*
        for(int i=0; i<recentValueCount; i++) {
            if (i > (sz-1)) break;
            values.put(stats.getElement(i));
        }
        */
        obj.put("values", values);
        if (summary) {
            obj.put("min", stats.getMin());
            obj.put("max", stats.getMax());
            obj.put("mean", stats.getMean());
            obj.put("100th", stats.getPercentile(99));
            obj.put("99th", stats.getPercentile(99));
            obj.put("95th", stats.getPercentile(99));
            obj.put("variance", stats.getVariance());
            obj.put("stddev", stats.getStandardDeviation());
            obj.put("kurtosis", stats.getKurtosis());
            obj.put("skewness", stats.getSkewness());
            obj.put("sumsq", stats.getSumsq());
        }
        return obj;
    }
    
}


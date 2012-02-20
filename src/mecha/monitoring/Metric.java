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
    
}


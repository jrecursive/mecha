package mecha.monitoring;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class Rates {
    ConcurrentHashMap<String, AtomicInteger> rateMap;
    
    public Rates() {
        rateMap = new ConcurrentHashMap<String, AtomicInteger>();
    }
    
    public void add(String name) {
        if (!rateMap.containsKey(name)) {
            rateMap.put(name, new AtomicInteger(0));
        }
        AtomicInteger value = rateMap.get(name);
        value.incrementAndGet();
    }
    
    public void clear() {
        synchronized(rateMap) {
            for(String k : rateMap.keySet()) {
                rateMap.get(k).set(0);
            }
        }
    }
    
    protected ConcurrentHashMap<String, AtomicInteger> getRateMap() {
        return rateMap;
    }
}

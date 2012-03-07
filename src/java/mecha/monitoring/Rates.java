/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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

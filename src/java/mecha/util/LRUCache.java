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

package mecha.util;

import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

public class LRUCache<K,V> {
    final private static float hashTableLoadFactor = 0.75f;
    private LinkedHashMap<K,V> map;
    private int cacheSize;

    public LRUCache (int cacheSize) {
       this.cacheSize = cacheSize;
       int hashTableCapacity = (int)Math.ceil(cacheSize / hashTableLoadFactor) + 1;
       map = new LinkedHashMap<K,V>(hashTableCapacity, hashTableLoadFactor, true) {
          private static final long serialVersionUID = 1;
          @Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
             return size() > LRUCache.this.cacheSize; 
          }
       }; 
    }
    
    public V get (K key) {
       return map.get(key); }
    
    public void put (K key, V value) {
       map.put (key,value); }
    
    public void clear() {
       map.clear(); }
    
    public int usedEntries() {
       return map.size(); }

    public Collection<Map.Entry<K,V>> getAll() {
       return new ArrayList<Map.Entry<K,V>>(map.entrySet()); }
}
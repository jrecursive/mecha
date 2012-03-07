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

package mecha.db;

public interface BucketDriver {

    public void stop() throws Exception;
    
    public byte[] get(byte[] key) throws Exception;
    
    public void put(byte[] key, byte[] value) throws Exception;    
    
    public void delete(byte[] key) throws Exception;
    
    public boolean foreach(MDB.ForEachFunction forEachFunction) throws Exception;
    
    public long count() throws Exception;
        
    public boolean isEmpty() throws Exception;
        
    public void drop() throws Exception;
        
    public void commit();
}

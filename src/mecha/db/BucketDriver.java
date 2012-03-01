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

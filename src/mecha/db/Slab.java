package mecha.db;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.concurrent.*;
import java.util.logging.*;

import mecha.json.*;

public class Slab {
    final private static Logger log = 
        Logger.getLogger(Slab.class.getName());

    final public static boolean O_READONLY = true;
    final public static boolean O_READWRITE = false;
    
    final private String filename;
    final private RandomAccessFile raf;
    final private CRC32 crc = new CRC32();
    
    final static private int REC_BUF_SZ = 262144;
    final private byte[] cTmp = new byte[REC_BUF_SZ];
    final private Inflater decompressor = new Inflater();
    final private Deflater compressor = new Deflater();
    final private boolean useCompression;

    public Slab(String fn) throws Exception {
        this(fn, false);
    }
    
    public Slab(String fn, String mode) throws Exception {
        this(fn, false, mode);
    }

    public Slab(String fn, boolean useCompression) throws Exception {
        this.filename = fn;
        this.useCompression = useCompression;
        raf = new RandomAccessFile(fn, "r");
        if (useCompression) initCompression();
    }
    
    public Slab(String fn, boolean useCompression, String mode) throws Exception {
        this.filename = fn;
        this.useCompression = useCompression;
        raf = new RandomAccessFile(fn, mode);
        if (useCompression) initCompression();
    }
    
    public synchronized byte[] get(long key) throws Exception {
        try {
            if (key > raf.length()) {
                throw new Exception("Read past end of file: " +
                        key + " > " + raf.length());
            }
            raf.seek(key);
            byte[] bytes = new byte[4];
            raf.read(bytes);
            
            int rLen = (((bytes[3] & 0xff) << 24) | ((bytes[2] & 0xff) << 16)
                       | ((bytes[1] & 0xff) << 8) | (bytes[0] & 0xff));
            
            byte[] cRec = new byte[rLen];
            if (rLen != raf.read(cRec)) {
                throw new Exception("Corrupted Index: " +
                        this.filename + "!");
            }
            return cRec;
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new Exception(ex);
        }
    }

    public String getString(long key) throws Exception {
        byte[] bytes = get(key);
        if (useCompression) {
            return decompress(bytes);
        } else {
            return new String(bytes);
        }
    }
    
    public JSONObject getObject(long key) throws Exception {
        String jsonStr = getString(key);
        JSONObject jo = new JSONObject(jsonStr);
        return jo;
    }

    public synchronized long append(byte[] record) throws Exception {
        byte[] cRec;
        long key = raf.length();
        raf.seek(key);
        if (useCompression) {
            cRec = compress(record);
        } else {
            cRec = record;
        }
        int rLen = cRec.length;
        byte[] rLen_b = new byte[4];
        rLen_b[3] = (byte) (0xFF & (rLen >> 24));
        rLen_b[2] = (byte) (0xFF & (rLen >> 16));
        rLen_b[1] = (byte) (0xFF & (rLen >> 8));
        rLen_b[0] = (byte) (0xFF & rLen);
        raf.write(rLen_b);
        raf.write(cRec);
        return key;
    }
    
    private boolean checksum(long cs, String r)
            throws Exception {
        crc.update(r.getBytes("UTF-8"));
        boolean verified = crc.getValue() == cs;
        crc.reset();
        return verified;
    }

    private long readChecksum() throws Exception {
        return raf.readLong();
    }

    private void writeChecksum(String s) throws Exception {
        crc.update(s.getBytes("UTF-8"));
        raf.writeLong(crc.getValue());
        crc.reset();
    }

    private String readUTF() throws Exception {
        return raf.readUTF();
    }

    private void writeUTF(String s) throws Exception {
        raf.writeUTF(s);
    }

    public synchronized void close() throws Exception {
        raf.close();
    }

    public long size() throws Exception {
        return raf.length();
    }

    public synchronized void sync() throws Exception {
        raf.getChannel().force(false);
    }
    
    /*
     * gzip compression
    */
    
    private String decompress(byte[] bytes) throws Exception {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes); 
        BufferedInputStream bis = new BufferedInputStream(bin);
        InflaterInputStream gzin = new InflaterInputStream(bis);
        
        StringBuffer sb = new StringBuffer();
        byte[] buf = new byte[4192];
        int len;
        while ((len = gzin.read(buf)) > 0) {
            sb.append(new String(buf, 0, len));
        }
        return sb.toString();
    }
    
    private void initCompression() {
        compressor.setLevel(Deflater.BEST_COMPRESSION);
    }

    private byte[] compress(byte[] value) throws Exception {
        Deflater deflater = new Deflater();
        deflater.setInput(value);
        deflater.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(value.length);
        byte[] buffer = new byte[1024];
        while(!deflater.finished()) {
            int bytesCompressed = deflater.deflate(buffer);
            bos.write(buffer,0,bytesCompressed);
        }
        bos.close();
        return bos.toByteArray();
    }

    private byte[] compress2(byte[] value) {
        compressor.setInput(value);
        compressor.finish();
        Arrays.fill(cTmp, (byte) 0);
        int cSz = compressor.deflate(cTmp);
        byte[] compressedValue = new byte[cSz];
        compressedValue = Arrays.copyOf(cTmp, cSz);
        compressor.reset();
        return compressedValue;
    }
    
    //
    // this method is actually slower than the inputstream method
    //
    private String decompress2(byte[] cVal) throws Exception {
        decompressor.setInput(cVal, 0, cVal.length);
        Arrays.fill(cTmp, (byte) 0);
        int rLen = decompressor.inflate(cTmp);
        byte[] dcVal = new byte[rLen];
        String r = new String(cTmp, 0, rLen);
        decompressor.reset();
        return r;
    }
    
    /*
     * sequential iterator
    */
    
    public interface SlabIterator {
        public void finish(Slab slab, String fn) throws Exception;
        public boolean process(Slab slab, String fn, long offset, int recnum, JSONObject jo) throws Exception;
    }
    
    public void iterate(SlabIterator slabIterator) throws Exception {
        iterate(slabIterator, 0);
    }
    
    public int iterate(SlabIterator slabIterator, long startPos) throws Exception {
        raf.seek(startPos);
        
        int recnum = 0;
        while(true) {
            try {
                long pos = raf.getChannel().position();
                if (pos == raf.length()) break;
                JSONObject jo = getObject(pos);
                boolean continueProcessing = slabIterator.process(this, filename, pos, recnum, jo);
                if (!continueProcessing) break;
                recnum++;
                // getObject advances the pointer
            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }
        return recnum;
    }
    
    /*
     * threaded iterator
    */
    
    private class SlabIteratorObject {
        public Slab slab;
        public byte[] obj;
        public int recnum;
        public String fn;
        public long offset;
    }
    
    public int iterate2(final SlabIterator si) throws Exception {
        int processors = Runtime.getRuntime().availableProcessors()-1;
        final BlockingQueue<SlabIteratorObject> queue = 
            new ArrayBlockingQueue<SlabIteratorObject>(processors * 1000);
        ExecutorService es = Executors.newFixedThreadPool(processors);
        
        for(int i=0; i<processors; i++) {
            es.execute(new Runnable() {
                public void run() {
                    while(true) {
                        try {
                            SlabIteratorObject sib = queue.take();
                            // todo: react to boolean return value
                            JSONObject obj = new JSONObject(new String(decompress(sib.obj)));
                            si.process(sib.slab, sib.fn, sib.offset, sib.recnum, obj);
                        } catch (java.lang.InterruptedException iex) {
                            break;
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            //break;
                        }
                    }
                }
            });
        }
        
        long t_st = System.currentTimeMillis();
        raf.seek(0);
        int recnum = 0;
        while(true) {
            try {
                long pos = raf.getChannel().position();
                if (pos == raf.length()) break;
                byte[] bytes = get(pos);
                SlabIteratorObject sib = new SlabIteratorObject();
                sib.fn = filename;
                sib.recnum = recnum;
                sib.slab = this;
                sib.offset = pos;
                sib.obj = bytes;
                queue.put(sib);
                recnum++;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        log.info("waiting for queue to empty...");
        
        while(queue.size() > 0) {
            Thread.sleep(250);
        }
        
        log.info("finishing...");
        si.finish(this, filename);
        
        long t_end = System.currentTimeMillis();
        double t_seconds = ( (t_end - t_st) / 1000.00 );
        double p_rate = ( (double) recnum ) / t_seconds;
        log.info("#/processed = " + recnum + " in " + t_seconds + " seconds (" + p_rate + " / sec)");

        es.shutdownNow();
        
        return recnum;
    }

    public static void main(String args[]) throws Exception {
        Slab slab = new Slab("/c/slabs/consolidated.research.slab");
        byte[] testRecord = slab.get(0);
        log.info("testRecord.length = " + testRecord.length);
        String decompressedRecord = slab.getString(0);
        log.info("decompressedRecord = " + decompressedRecord);
        JSONObject jo = slab.getObject(0);
        log.info("jo = " + jo.toString(4));
        
        int processed = 
            slab.iterate2(new SlabIterator() {
                public boolean process(Slab slab, String fn, long offset, int recnum, JSONObject jo) throws Exception {
                    //log.info("[" + recnum + "] fn = " + fn + ", offset = " + offset + " jo.name = " + jo.getString("name"));
                    return true;
                }
                
            public void finish(Slab slab, String fn) throws Exception {
            }
            
            });
        slab.close();
    }
}




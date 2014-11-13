package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class AsyncFIFOFileWriter implements Runnable {
    protected CopyOnWriteArrayList<ConcurrentLinkedQueue<ICachedPage>> queues = new CopyOnWriteArrayList<ConcurrentLinkedQueue<ICachedPage>>();
    Thread writer;
    boolean haltWriter = true;
    IIOManager ioManager;
    IBufferCache bufferCache;
    private static final Logger LOGGER = Logger.getLogger(AsyncFIFOFileWriter.class.getName());
    
    public AsyncFIFOFileWriter(IIOManager ioManager, IBufferCache bufferCache){
        this.ioManager = ioManager;
        this.bufferCache = bufferCache;
    }
    
    public ConcurrentLinkedQueue<ICachedPage> createQueue() {
        ConcurrentLinkedQueue<ICachedPage> queue = new ConcurrentLinkedQueue<ICachedPage>();
        queues.add(queue);
        
        if(writer == null) {
            synchronized(this) {
                if(writer == null) {
                    writer = new Thread(this);
                    haltWriter = false;
                    writer.start();
                }
            }
        }
        
        return queue;
    }
    
    public void finishQueue(ConcurrentLinkedQueue<ICachedPage> queue) {
        System.out.println("[FIFO] Finishing Queue");
        try {
            synchronized(queue) {
                queue.wait();
            }
            queues.remove(queue);
            if(queues.size() == 0) {
                synchronized(this) {
                    if(queues.size() == 0) {
                        haltWriter = true;
                        writer.join();
                        System.out.println("[FIFO] Writer stopped");
                        writer = null;
                    }
                }
            }
        } catch (InterruptedException e) {
            // TODO what do we do here?
            e.printStackTrace();
        }
        System.out.println("[FIFO] Queue finished");
    }
    
    private void writePage(ICachedPage page){
        CachedPage castedPage = (CachedPage) page;
        if(castedPage.latch.writeLock().tryLock()){
            try{
                bufferCache.write(page);
                bufferCache.returnPage(page);
            }catch(HyracksDataException e){
                LOGGER.severe("Error while attempting to flush FIFO page" + e);
            }

        }else{
            throw new IllegalStateException("Confiscated page:"+castedPage.dpid+" is write latched and cannot be written");
        }
    }
    
    public static void setDpid(ICachedPage page, long dpid){
        ((CachedPage)page).dpid = dpid;
    }
    

    @Override
    public void run() {
        System.out.println("[FIFO] Writer started");
        while(!haltWriter) {
            //System.out.println("[FIFO] Poll");
            boolean success = false;
            for(ConcurrentLinkedQueue<ICachedPage> queue : queues) {
                ICachedPage page = queue.poll();
                if(page == null) {
                    synchronized(queue) {
                        queue.notify();
                    }
                } else {
                    writePage(page);
                    System.out.println("[FIFO] Flush " + page);
                    success = true;
                }
            }
            if(!success) {
                try {
                    //System.out.println("[FIFO] Sleep");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
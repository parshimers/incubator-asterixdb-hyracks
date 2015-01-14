package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class AsyncFIFOPageQueueManager implements Runnable {
    private final static boolean DEBUG = false;
    
    protected class QueueEntry {
        ICachedPage page;
        int fileid = -1;
        IFIFOPageWriter writer;
        IBufferCache bufferCache;
        protected QueueEntry(ICachedPage page, int fileid, IFIFOPageWriter writer, IBufferCache bufferCache)  {
            this.page = page;
            this.fileid = fileid;
            this.writer = writer;
            this.bufferCache = bufferCache;
        }
    }
    
    protected class PageQueue implements IFIFOPageQueue {
        final ConcurrentLinkedQueue<ICachedPage> pageQueue;
        final IBufferCache bufferCache;
        final IFIFOPageWriter writer;
        int fileid = -1;
        
        protected PageQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
            if(DEBUG) System.out.println("[FIFO] New Queue");
            this.pageQueue = new ConcurrentLinkedQueue<ICachedPage>();
            this.bufferCache = bufferCache;
            this.writer = writer;
        }

        protected ConcurrentLinkedQueue<ICachedPage> getPageQueue() {
            return pageQueue;
        }

        protected IBufferCache getBufferCache() {
            return bufferCache;
        }

        protected IFIFOPageWriter getWriter() {
            return writer;
        }

        public void setFileId(int fileid) {
            this.fileid = fileid;
        }

        public int getFileId() {
            return fileid;
        }

        @Override
        public void put(ICachedPage page) {
            try {
                queue.put(new QueueEntry(page, 
                          BufferedFileHandle.getFileId(((CachedPage)page).dpid), 
                          writer, bufferCache));
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    protected LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();
    Thread writerThread;
    boolean haltWriter = true;
    
    public PageQueue createQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
        if (writerThread == null) {
            synchronized (this) {
                if (writerThread == null) {
                    writerThread = new Thread(this);
                    haltWriter = false;
                    writerThread.start();
                }
            }
        }
        
        return new PageQueue(bufferCache, writer);
    }

    public static void setDpid(ICachedPage page, long dpid) {
        ((CachedPage) page).dpid = dpid;
    }

    public void finishQueue(IFIFOPageQueue pageQueue) {
        if(DEBUG) System.out.println("[FIFO] Finishing Queue");
        try {
            synchronized (queue) {
               if(DEBUG)  System.out.println("Waiting for " + pageQueue);
               if(!queue.isEmpty()){
                  queue.wait();
               }
            }
        } catch (InterruptedException e) {
            // TODO what do we do here?
            e.printStackTrace();
        }
        if(DEBUG) System.out.println("[FIFO] Queue finished");
    }

    @Override
    public void run() {
        if(DEBUG) System.out.println("[FIFO] Writer started");
        while (!haltWriter) {
            try {
                QueueEntry entry = queue.take();
                ICachedPage page = entry.page;
                
                if(DEBUG) System.out.println("[FIFO] Write " + ((CachedPage)page).dpid);

                try {
                    entry.writer.write(page, entry.bufferCache);
                } catch (HyracksDataException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                if(queue.isEmpty()) {
                    synchronized(queue) {
                        queue.notifyAll(); // TODO not 100% threadsafe
                    }
                }
            } catch(InterruptedException e) {
                // TODO
            }
        }
    }
}
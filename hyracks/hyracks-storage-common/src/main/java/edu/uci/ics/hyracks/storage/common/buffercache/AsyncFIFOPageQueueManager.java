package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class AsyncFIFOPageQueueManager implements Runnable {
    private final static boolean DEBUG = false;
    
    protected class QueueEntry {
        ICachedPage page;
        int fileid = -1;
        boolean notifier = false;
        IFIFOPageWriter writer;
        IBufferCache bufferCache;
        protected QueueEntry(ICachedPage page, int fileid, IFIFOPageWriter writer, IBufferCache bufferCache)  {
            this.page = page;
            this.fileid = fileid;
            this.writer = writer;
            this.bufferCache = bufferCache;
        }
        protected QueueEntry(boolean notifier)  {
            this.page = null;
            this.fileid = -1;
            this.writer = null;
            this.bufferCache = null;
            this.notifier = notifier;
        }
    }
    
    protected class PageQueue implements IFIFOPageQueue {
        final ConcurrentLinkedQueue<ICachedPage> pageQueue;
        final IBufferCache bufferCache;
        final IFIFOPageWriter writer;
        final ConcurrentHashMap<Integer, AtomicInteger> highOffsets;
        int fileid = -1;

        protected PageQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
            if(DEBUG) System.out.println("[FIFO] New Queue");
            this.pageQueue = new ConcurrentLinkedQueue<ICachedPage>();
            this.highOffsets = new ConcurrentHashMap<Integer, AtomicInteger>();
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
    volatile Thread writerThread;
    boolean haltWriter = true;
    AtomicBoolean sleeping = new AtomicBoolean();

    public PageQueue createQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
        if (writerThread == null) {
            synchronized(this){
                if (writerThread == null) {
                    writerThread = new Thread(this);
                    writerThread.setName("FIFO Writer Thread");
                    haltWriter = false;
                    writerThread.start();
                }
            }
        }
        return new PageQueue(bufferCache, writer);
    }
    public void destroyQueue(){
        haltWriter = true;
        if(writerThread!=null){
            while(!queue.isEmpty()){
                synchronized(queue){
                    try {
                        queue.wait(100l);
                    }catch(InterruptedException e){
                        break;
                    }
                }
            }
            try {
                writerThread.interrupt();
                writerThread.join();
            }catch(InterruptedException e){
                // that's ok?
            }
        }
    }

    public static void setDpid(ICachedPage page, long dpid) {
        ((CachedPage) page).dpid = dpid;
    }

    public void finishQueue() {
        if(DEBUG) System.out.println("[FIFO] Finishing Queue");
        try {
            if(queue.isEmpty()){
                return;
            }
            //else
            QueueEntry lowWater = new QueueEntry(true);
            queue.put(lowWater);
            while(queue.contains(lowWater)){
                synchronized(lowWater){
                    lowWater.wait(100l);
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
            ICachedPage page = null;
            try {
                QueueEntry entry = queue.take();
                if(entry.notifier == true){
                    synchronized(entry) {
                        entry.notifyAll();
                        continue;
                    }
                }
                page = entry.page;
                page.acquireReadLatch();
                
                if(DEBUG) System.out.println("[FIFO] Write " + BufferedFileHandle.getFileId(((CachedPage)page).dpid)+","
                        + BufferedFileHandle.getPageId(((CachedPage)page).dpid));

                try {
                    entry.writer.write(page, entry.bufferCache);
                } catch (HyracksDataException e) {
                    //TODO: What do we do, if we could not write the page?
                    e.printStackTrace();
                }
            } catch(InterruptedException e) {
                continue;
            } finally{
                if(page!=null){
                    page.releaseReadLatch();
                }
            }
        }
    }
}
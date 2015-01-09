package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class AsyncFIFOPageQueueManager implements Runnable {
    private final static boolean DEBUG = false;
    protected class Queue {
        final ConcurrentLinkedQueue<ICachedPage> pageQueue;
        final IBufferCache bufferCache;
        final IFIFOPageWriter writer;
        int fileid = -1;

        protected Queue(IBufferCache bufferCache, IFIFOPageWriter writer) {
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
    }

    protected CopyOnWriteArrayList<Queue> queues = new CopyOnWriteArrayList<Queue>();
    Thread writerThread;
    boolean haltWriter = true;

    public ConcurrentLinkedQueue<ICachedPage> createQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
        Queue queue = new Queue(bufferCache, writer);
        queues.add(queue);

        if (writerThread == null) {
            synchronized (this) {
                if (writerThread == null) {
                    writerThread = new Thread(this);
                    haltWriter = false;
                    writerThread.start();
                }
            }
        }

        return queue.getPageQueue();
    }

    public static void setDpid(ICachedPage page, long dpid) {
        ((CachedPage) page).dpid = dpid;
    }

    public void finishQueue(ConcurrentLinkedQueue<ICachedPage> pageQueue) {
       if(DEBUG)  System.out.println("[FIFO] Finishing Queue");
        try {
            synchronized (pageQueue) {
               if(DEBUG)  System.out.println("Waiting for " + pageQueue);
                pageQueue.wait();
            }
            for (Queue queue : queues) {
                boolean removed = false;
                if (queue.getPageQueue() == pageQueue) {
                    removed = queues.remove(queue);
                    //if (queue.getFileId() != -1){
                        //queue.getWriter().sync(queue.getFileId(), queue.getBufferCache());
                    //}
                   if(DEBUG)  System.out.println("[FIFO] Removed? " + removed);
                    break;
                }
                //assert (removed);
            }
            /*
            if (queues.size() == 0) {
                synchronized (this) {
                    if (queues.size() == 0) {
                        haltWriter = true;
                        writerThread.join();
                       if(DEBUG)  System.out.println("[FIFO] Writer stopped");
                        writerThread = null;
                    }
                }
            }
            */

        } catch (InterruptedException e) {
            // TODO what do we do here?
            e.printStackTrace();
        }
        if(DEBUG) System.out.println("[FIFO] Queue finished");
    }

    @Override
    public void run() {
        if(DEBUG) System.out.println("[FIFO] Writer started");
        long lastDpid = 0;
        while (!haltWriter) {
            //System.out.println("[FIFO] Poll");
            boolean success = false;
            for (Queue queue : queues) {
                ICachedPage page = queue.getPageQueue().poll();
                if (page == null) {
                    synchronized (queue.getPageQueue()) {
                        queue.getPageQueue().notifyAll();
                    }
                } else {
                  if(DEBUG)   System.out.println("[FIFO] Write " + (((CachedPage) page).dpid >> 32) + ":"
                            + (((((CachedPage) page).dpid) << 32) >> 32));
                    queue.setFileId(BufferedFileHandle.getFileId(((CachedPage) page).dpid));
                    try {
                        queue.getWriter().write(page, queue.getBufferCache());
                        lastDpid = ((CachedPage)page).dpid;
                    } catch (HyracksDataException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    success = true;
                }
            }
            /*
            if (!success) {
                try {
                    //System.out.println("[FIFO] Sleep");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            */
        }
    }
}
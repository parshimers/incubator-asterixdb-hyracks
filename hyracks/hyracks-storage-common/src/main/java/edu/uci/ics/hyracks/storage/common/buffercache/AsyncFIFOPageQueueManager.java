package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFileHandle;

public class AsyncFIFOPageQueueManager implements Runnable {
    protected class Queue {
        final ConcurrentLinkedQueue<ICachedPage> pageQueue;
        final IBufferCache bufferCache;
        final IFIFOPageWriter writer;
        
        protected Queue(IBufferCache bufferCache, IFIFOPageWriter writer) {
            System.out.println("[FIFO] New Queue");
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
    }
    
    protected CopyOnWriteArrayList<Queue> queues = new CopyOnWriteArrayList<Queue>();
    Thread writerThread;
    boolean haltWriter = true;
    
    public ConcurrentLinkedQueue<ICachedPage> createQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
        Queue queue = new Queue(bufferCache, writer);
        queues.add(queue);
        
        if(writerThread == null) {
            synchronized(this) {
                if(writerThread == null) {
                    writerThread = new Thread(this);
                    haltWriter = false;
                    writerThread.start();
                }
            }
        }
        
        return queue.getPageQueue();
    }
    
    public void finishQueue(ConcurrentLinkedQueue<ICachedPage> pageQueue) {
        System.out.println("[FIFO] Finishing Queue");
        try {
            synchronized(pageQueue) {
                System.out.println("Waiting for " + pageQueue);
                pageQueue.wait();
            }
            for(Queue queue : queues) {
                if(queue.getPageQueue() == pageQueue) {
                    boolean removed = queues.remove(queue);
                    assert(removed);
                    System.out.println("[FIFO] Removed? " + removed);
                    break;
                }
            }
            if(queues.size() == 0) {
                synchronized(this) {
                    if(queues.size() == 0) {
                        haltWriter = true;
                        writerThread.join();
                        System.out.println("[FIFO] Writer stopped");
                        writerThread = null;
                    }
                }
            }
        } catch (InterruptedException e) {
            // TODO what do we do here?
            e.printStackTrace();
        }
        System.out.println("[FIFO] Queue finished");
    }

    @Override
    public void run() {
        System.out.println("[FIFO] Writer started");
        while(!haltWriter) {
            System.out.println("[FIFO] Poll");
            boolean success = false;
            for(Queue queue : queues) {
                System.out.println("Probing " + queue);
                ICachedPage page = queue.getPageQueue().poll();
                if(page == null) {
                    synchronized(queue.getPageQueue()) {
                        queue.getPageQueue().notifyAll();
                    }
                } else {
                    System.out.println("[FIFO] Write " + page);
                    try {
                        queue.getWriter().write(page, queue.getBufferCache());
                    } catch (HyracksDataException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    success = true;
                }
            }
            if(!success) {
                try {
                    System.out.println("[FIFO] Sleep");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
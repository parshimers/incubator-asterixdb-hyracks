/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.common.buffercache;

import java.nio.Buffer;
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

    protected LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();
    volatile Thread writerThread;
    protected AtomicBoolean poisoned = new AtomicBoolean(false);
    protected BufferCache bufferCache;
    protected PageQueue pageQueue;

    public AsyncFIFOPageQueueManager(BufferCache bufferCache){
        this.bufferCache = bufferCache;
    }

    protected class QueueEntry {
        ICachedPage page;
        int fileid = -1;
        boolean notifier = false;
        boolean poison = false;
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
        protected QueueEntry(boolean notifier, boolean poison){
            this.page = null;
            this.fileid = -1;
            this.writer = null;
            this.bufferCache = null;
            this.notifier = notifier;
            this.poison = poison;
        }
    }
    
    protected class PageQueue implements IFIFOPageQueue {
        final IBufferCache bufferCache;
        final IFIFOPageWriter writer;

        protected PageQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
            if(DEBUG) System.out.println("[FIFO] New Queue");
            this.bufferCache = bufferCache;
            this.writer = writer;
        }

        protected IBufferCache getBufferCache() {
            return bufferCache;
        }

        protected IFIFOPageWriter getWriter() {
            return writer;
        }

        @Override
        public void put(ICachedPage page) throws HyracksDataException {
            try {
                if(!poisoned.get()) {
                    queue.put(new QueueEntry(page,
                            BufferedFileHandle.getFileId(((CachedPage) page).dpid),
                            writer, bufferCache));
                }
                else{
                    throw new HyracksDataException("Queue is closing");
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    public PageQueue createQueue(IFIFOPageWriter writer) {
        if (writerThread == null) {
            synchronized(this){
                if (writerThread == null) {
                    writerThread = new Thread(this);
                    writerThread.setName("FIFO Writer Thread");
                    writerThread.start();
                    pageQueue = new PageQueue(bufferCache,writer);
                }
            }
        }
        return pageQueue;
    }

    public void destroyQueue(){
        poisoned.set(true);
        QueueEntry poisonPill = new QueueEntry(true,true);
        if(writerThread == null){
            synchronized (this){
                if(writerThread == null) {
                    return;
                }
            }
        }

        try{
            synchronized(poisonPill){
                queue.put(poisonPill);
                while(queue.contains(poisonPill)){
                    poisonPill.wait();
                }
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void setDpid(ICachedPage page, long dpid) {
        ((CachedPage) page).dpid = dpid;
    }

    public void finishQueue() {
        if(DEBUG) System.out.println("[FIFO] Finishing Queue");
        try {
            QueueEntry lowWater = new QueueEntry(true);
            synchronized(lowWater){
                queue.put(lowWater);
                while(queue.contains(lowWater)){
                        lowWater.wait();
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
        boolean die = false;
        while (!die) {
            ICachedPage page = null;
            try {
                QueueEntry entry = queue.take();
                if(entry.notifier == true){
                    synchronized(entry) {
                        if(entry.poison) { die = true; }
                        entry.notifyAll();
                        continue;
                    }
                }
                page = entry.page;

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
            }
        }
    }
}
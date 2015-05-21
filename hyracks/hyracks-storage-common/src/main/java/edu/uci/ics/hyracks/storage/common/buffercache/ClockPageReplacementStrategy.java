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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClockPageReplacementStrategy implements IPageReplacementStrategy {
    private static final int MAX_UNSUCCESSFUL_CYCLE_COUNT = 3;

    private IBufferCacheInternal bufferCache;
    private AtomicInteger clockPtr;
    private ICacheMemoryAllocator allocator;
    private AtomicInteger numPages;
    private final int pageSize;
    private final int realMaxAllowedNumPages;
    private AtomicInteger maxAllowedNumPages;
    private AtomicInteger cpIdCounter;
    //DEBUG

    public ClockPageReplacementStrategy(ICacheMemoryAllocator allocator, int pageSize, int maxAllowedNumPages) {
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.maxAllowedNumPages = new AtomicInteger(maxAllowedNumPages);
        this.realMaxAllowedNumPages = maxAllowedNumPages;
        this.clockPtr = new AtomicInteger(0);
        this.numPages = new AtomicInteger(0);
        this.cpIdCounter = new AtomicInteger(0);
    }

    @Override
    public Object createPerPageStrategyObject(int cpid) {
        return new AtomicBoolean();
    }

    @Override
    public void setBufferCache(IBufferCacheInternal bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void notifyCachePageReset(ICachedPageInternal cPage) {
        getPerPageObject(cPage).set(false);
    }

    @Override
    public void notifyCachePageAccess(ICachedPageInternal cPage) {
        getPerPageObject(cPage).set(true);
    }

    @Override
    public ICachedPageInternal findVictim() {
        ICachedPageInternal cachedPage = null;
        if (numPages.get() >= maxAllowedNumPages.get()) {
            cachedPage = findVictimByEviction();
        } else {
            cachedPage = allocatePage();
        }
        return cachedPage;
    }

    private ICachedPageInternal findVictimByEviction() {
        //check if we're starved from confiscation
        if (maxAllowedNumPages.get() == 0)
            return null;
        int startClockPtr = clockPtr.get();
        int cycleCount = 0;
        do {
            ICachedPageInternal cPage = bufferCache.getPage(clockPtr.get());

            /*
             * We do two things here:
             * 1. If the page has been accessed, then we skip it -- The CAS would return
             * false if the current value is false which makes the page a possible candidate
             * for replacement.
             * 2. We check with the buffer manager if it feels its a good idea to use this
             * page as a victim.
             */
            AtomicBoolean accessedFlag = getPerPageObject(cPage);
            if (!accessedFlag.compareAndSet(true, false) && !cPage.getVictimized().get()) {
                if (cPage.pinIfGoodVictim()) {
                    //Resolve the race of returning a victim to two actors
                    if(cPage.getVictimized().compareAndSet(false,true)) {
                        return cPage;
                    }
                }
            }
            clockPtr.set(clockPtr.incrementAndGet() % (numPages.get()-1));
            if (clockPtr.get() == startClockPtr) {
                ++cycleCount;
            }
        } while (cycleCount < MAX_UNSUCCESSFUL_CYCLE_COUNT);
        return null;
    }

    @Override
    public int getNumPages() {
        return numPages.get();
    }


    public int addPage() {
        return numPages.incrementAndGet();
    }

    private ICachedPageInternal allocatePage() {
        CachedPage cPage = new CachedPage(cpIdCounter.getAndIncrement(), allocator.allocate(pageSize, 1)[0], this);
        bufferCache.addPage(cPage);
        numPages.incrementAndGet();
        AtomicBoolean accessedFlag = getPerPageObject(cPage);
        if (!accessedFlag.compareAndSet(true, false) && !cPage.getVictimized().get()) {
            if (cPage.pinIfGoodVictim()) {
                //Resolve the race of returning a victim to two actors
                if(cPage.getVictimized().compareAndSet(false,true)) {
                    return cPage;
                }
            }
        }
        return null;
    }

    private AtomicBoolean getPerPageObject(ICachedPageInternal cPage) {
        return (AtomicBoolean) cPage.getReplacementStrategyObject();
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getMaxAllowedNumPages() {
        return realMaxAllowedNumPages;
    }

    @Override
    public void adviseWontNeed(ICachedPageInternal cPage) {
        //make the page appear as if it wasn't accessed even if it was
        getPerPageObject(cPage).set(false);
    }
}
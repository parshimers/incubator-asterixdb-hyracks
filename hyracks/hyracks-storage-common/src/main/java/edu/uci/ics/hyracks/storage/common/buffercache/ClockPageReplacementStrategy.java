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
    private AtomicInteger numPages = new AtomicInteger(0);
    private final int pageSize;
    private AtomicInteger maxAllowedNumPages;
    private BlockingDeque<ICachedPageInternal> hatedPages;

    public ClockPageReplacementStrategy(ICacheMemoryAllocator allocator, int pageSize, int maxAllowedNumPages) {
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.maxAllowedNumPages = new AtomicInteger(maxAllowedNumPages);
        this.hatedPages = new LinkedBlockingDeque<ICachedPageInternal>();
        clockPtr = new AtomicInteger(0);
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
        //first, check if there is a hated page we can return.
        ICachedPageInternal hated = hatedPages.poll();
        if (hated != null) {
            return hated;
        }
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
            if (!accessedFlag.compareAndSet(true, false)) {
                if (cPage.pinIfGoodVictim()) {
                    return cPage;
                }
            }
            clockPtr.set(clockPtr.incrementAndGet() % numPages.get());
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

    public int subtractPage() {
        //if we're at the edge, push the clock pointer forward
        if (clockPtr.get() == numPages.get() - 1) {
            clockPtr.set(0);
        }
        maxAllowedNumPages.decrementAndGet();
        return numPages.decrementAndGet();
    }

    public int addPage() {
        return numPages.incrementAndGet();
    }

    @Override
    public void returnPage() {
        addPage();
        maxAllowedNumPages.incrementAndGet();
    }

    private ICachedPageInternal allocatePage() {
        CachedPage cPage = new CachedPage(numPages.get(), allocator.allocate(pageSize, 1)[0], this);
        bufferCache.addPage(cPage);
        numPages.incrementAndGet();
        AtomicBoolean accessedFlag = getPerPageObject(cPage);
        if (!accessedFlag.compareAndSet(true, false)) {
            if (cPage.pinIfGoodVictim()) {
                return cPage;
            }
        }
        return null;
    }

    public ICachedPageInternal allocateAndConfiscate() {
        if (numPages.get() < maxAllowedNumPages.get()) {
            maxAllowedNumPages.decrementAndGet();
            CachedPage cPage = new CachedPage(numPages.get(), allocator.allocate(pageSize, 1)[0], this);
            return cPage;
        } else
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
        return maxAllowedNumPages.get();
    }

    @Override
    public void adviseWontNeed(ICachedPageInternal cPage) {
        //just offer, if the page replacement policy is busy and doesn't want to listen then it's fine.
        hatedPages.offer(cPage);
    }
}
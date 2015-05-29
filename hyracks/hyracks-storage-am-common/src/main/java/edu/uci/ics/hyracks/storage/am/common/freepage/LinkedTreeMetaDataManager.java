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
package edu.uci.ics.hyracks.storage.am.common.freepage;

import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeMetaDataManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class LinkedTreeMetaDataManager implements ITreeMetaDataManager {

    private static final byte META_PAGE_LEVEL_INDICATOR = -1;
    private static final byte FREE_PAGE_LEVEL_INDICATOR = -2;
    private final IBufferCache bufferCache;
    private int headPage = -1;
    private int fileId = -1;
    private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
    private boolean appendOnly = false;
    ICachedPage confiscatedMetaNode;
    ICachedPage filterPage;
    private static Logger LOGGER = Logger
            .getLogger("edu.uci.ics.hyracks.storage.am.common.freepage.LinkedTreeMetaDataManager");

    public LinkedTreeMetaDataManager(IBufferCache bufferCache, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        this.bufferCache = bufferCache;
        this.metaDataFrameFactory = metaDataFrameFactory;
        this.confiscatedMetaNode = null;
    }

    @Override
    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException {

        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        metaNode.acquireWriteLatch();

        try {
            metaFrame.setPage(metaNode);

            if (metaFrame.hasSpace()) {
                metaFrame.addFreePage(freePage);
            } else {
                // allocate a new page in the chain of meta pages
                int newPage = metaFrame.getFreePage();
                if (newPage < 0) {
                    throw new Exception("Inconsistent Meta Page State. It has no space, but it also has no entries.");
                }

                ICachedPage newNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newPage), false);
                newNode.acquireWriteLatch();

                try {
                    int metaMaxPage = metaFrame.getMaxPage();

                    // copy metaDataPage to newNode
                    System.arraycopy(metaNode.getBuffer().array(), 0, newNode.getBuffer().array(), 0, metaNode
                            .getBuffer().capacity());

                    metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
                    metaFrame.setNextPage(newPage);
                    metaFrame.setMaxPage(metaMaxPage);
                    metaFrame.addFreePage(freePage);
                } finally {
                    newNode.releaseWriteLatch(true);
                    bufferCache.unpin(newNode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }

        metaNode.acquireWriteLatch();

        int freePage = -1;
        try {
            metaFrame.setPage(metaNode);
            freePage = metaFrame.getFreePage();
            if (freePage < 0) { // no free page entry on this page
                int nextPage = metaFrame.getNextPage();
                if (nextPage > 0) { // sibling may have free pages
                    ICachedPage nextNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextPage), false);

                    nextNode.acquireWriteLatch();
                    // we copy over the free space entries of nextpage into the
                    // first meta page (metaDataPage)
                    // we need to link the first page properly to the next page
                    // of nextpage
                    try {
                        // remember entries that remain unchanged
                        int maxPage = metaFrame.getMaxPage();

                        // copy entire page (including sibling pointer, free
                        // page entries, and all other info)
                        // after this copy nextPage is considered a free page
                        System.arraycopy(nextNode.getBuffer().array(), 0, metaNode.getBuffer().array(), 0, nextNode
                                .getBuffer().capacity());

                        // reset unchanged entry
                        metaFrame.setMaxPage(maxPage);

                        freePage = metaFrame.getFreePage();
                        // sibling also has no free pages, this "should" not
                        // happen, but we deal with it anyway just to be safe
                        if (freePage < 0) {
                            freePage = nextPage;
                        } else {
                            metaFrame.addFreePage(nextPage);
                        }
                    } finally {
                        if (!appendOnly) {
                            metaNode.releaseWriteLatch(true);
                            bufferCache.unpin(metaNode);
                        } else {
                            metaNode.releaseWriteLatch(false);
                        }
                    }
                } else {
                    freePage = metaFrame.getMaxPage();
                    if (!appendOnly) {
                        freePage++;
                        metaFrame.setMaxPage(freePage);
                    } else {
                        metaFrame.setMaxPage(freePage + 1);
                    }
                }
            }
        } finally {
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }

        return freePage;
    }

    @Override
    public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        metaNode.acquireReadLatch();
        int maxPage = -1;
        try {
            metaFrame.setPage(metaNode);
            maxPage = metaFrame.getMaxPage();
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly) {
                bufferCache.unpin(metaNode);
            }
        }
        return maxPage;
    }

    @Override
    public void setFilterPageId(int filterPageId) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSMComponentFilterPageId(filterPageId);
        } finally {
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }
    }

    @Override
    public int getFilterPageId() throws HyracksDataException {
        ICachedPage metaNode;
        int filterPageId = BufferCache.INVALID_DPID;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            filterPageId = metaFrame.getLSMComponentFilterPageId();
            if(appendOnly && filterPageId == -1){
                //hint to filter manager that we are in append-only mode
                filterPageId = -2;
            }
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly) {
                bufferCache.unpin(metaNode);
            }
        }
        return filterPageId;
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        // initialize meta data page
        int metaPage = getFirstMetadataPage();
        if(metaPage == -1){
            throw new HyracksDataException("No valid metadata found in this file.");
        }
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), true);

        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
            metaFrame.setMaxPage(currentMaxPage);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        if (confiscatedMetaNode != null) { // don't init twice
            return;
        }
        ICachedPage metaNode = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
            metaFrame.setMaxPage(0);
        } finally {
            metaNode.releaseWriteLatch(false);
            confiscatedMetaNode = metaNode;
            appendOnly = true;
        }
    }

    @Override
    public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
        return metaDataFrameFactory;
    }

    @Override
    public byte getFreePageLevelIndicator() {
        return FREE_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public byte getMetaPageLevelIndicator() {
        return META_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame) {
        return metaFrame.getLevel() == FREE_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame) {
        return metaFrame.getLevel() == META_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void close() throws HyracksDataException {
        closeGivePageId();
    }

    private void writeFilterPage() throws HyracksDataException {
        if(filterPage != null) {
            ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
            metaFrame.setPage(confiscatedMetaNode);
            metaFrame.setValid(true);
            int finalFilterPage = getMaxPage(metaFrame);
            ICachedPage finalFilter = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, finalFilterPage), true);
            try {
                filterPage.acquireReadLatch();
                finalFilter.acquireWriteLatch();
                bufferCache.copyPage(filterPage, finalFilter);
                setFilterPageId(finalFilterPage);
            }
            finally {
                finalFilter.releaseWriteLatch(true);
                bufferCache.flushDirtyPage(finalFilter);
                bufferCache.unpin(finalFilter);
                filterPage.releaseReadLatch();
                bufferCache.returnPage(filterPage,false);
            }
        }
    }


    public int closeGivePageId() throws HyracksDataException {
        int finalPageId = 0;
        if (appendOnly && fileId > 0) {
            writeFilterPage();
            ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
            metaFrame.setPage(confiscatedMetaNode);
            metaFrame.setValid(true);
            int finalMetaPage = getMaxPage(metaFrame);
            ICachedPage finalMeta = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, finalMetaPage), true);
            try {
                confiscatedMetaNode.acquireReadLatch();
                finalMeta.acquireWriteLatch();
                bufferCache.copyPage(confiscatedMetaNode, finalMeta);
                finalPageId = finalMetaPage;
            } finally {
                finalMeta.releaseWriteLatch(true);
                bufferCache.flushDirtyPage(finalMeta);
                bufferCache.unpin(finalMeta);
                confiscatedMetaNode.releaseReadLatch();
                bufferCache.returnPage(confiscatedMetaNode, false);

            }

        }
        fileId = -1;
        return finalPageId;
    }

    /**
     * For storage on append-only media (such as HDFS), the meta data page has to be written last.
     * However, some implementations still write the meta data to the front. To deal with this as well
     * as to provide downward compatibility, this method tries to find the meta data page first in the
     * last and then in the first page of the file.
     *
     * @return The Id of the page holding the meta data
     * @throws HyracksDataException
     */
    @Override
    public int getFirstMetadataPage() throws HyracksDataException {
        if (headPage != -1)
            return headPage;

        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();

        int pages = bufferCache.getNumPagesOfFile(fileId);
        //look at the front (modify in-place index)
        int page = 0;
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, page), false);
        try {
            metaNode.acquireReadLatch();
            metaFrame.setPage(metaNode);

            if (isMetaPage(metaFrame)) {
                headPage = page;
                return headPage;
            }
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
        //otherwise, look at the back. (append-only index)
        page = pages-1 >0 ? pages -1 : 0;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, page), false);
        try {
            metaNode.acquireReadLatch();
            metaFrame.setPage(metaNode);

            if (isMetaPage(metaFrame)) {
                headPage = page;
                return headPage;
            }
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
        //if we find nothing, this isn't a tree (or isn't one yet).
        if(pages>0){
            return -1;
        }
        else{
            return 0;
        }
    }

    @Override
    public long getLSN() throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLSN();
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly) {
                bufferCache.unpin(metaNode);
            }
        }
    }

    @Override
    public void setLSN(long lsn) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSN(lsn);
        } finally {
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }
    }

    @Override
    public void setFilterPage(ICachedPage filterPage) {
        this.filterPage = filterPage;
    }

    @Override
    public ICachedPage getFilterPage() {
        return this.filterPage;
    }
}


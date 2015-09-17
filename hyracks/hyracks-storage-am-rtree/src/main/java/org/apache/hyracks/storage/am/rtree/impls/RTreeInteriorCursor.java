/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/************
 * This class is used to get all MBRs in a certain level (indicated by the second parameter, 
 * desiredLevelFromRoot of the constructor) in a R-tree.
 * This class exists for debugging purpose and must not be used for any other purposes.
 *  
 * @author kisskys
 *
 ************/
public class RTreeInteriorCursor implements ITreeIndexCursor {

    private int fileId = -1;
    private ICachedPage page = null;
    private IRTreeInteriorFrame interiorFrame = null;
    private IBufferCache bufferCache = null;

    private PathList pathList;
    private int rootPage;

    private int tupleIndex = 0;
    private int tupleIndexInc = 0;

    private ITreeIndexTupleReference frameTuple;
    private boolean readLatched = false;
    private final int desiredLevelFromRoot;
    private int rootLevel;

    public RTreeInteriorCursor(IRTreeInteriorFrame interiorFrame, int desiredLevelFromRoot) {
        this.interiorFrame = interiorFrame;
        this.desiredLevelFromRoot = desiredLevelFromRoot;
        this.frameTuple = interiorFrame.createTupleReference();
    }

    @Override
    public void close() throws HyracksDataException {
        if (readLatched) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            readLatched = false;
        }
        tupleIndex = 0;
        tupleIndexInc = 0;
        page = null;
        pathList = null;
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    protected boolean fetchNextPage() throws HyracksDataException {
        boolean succeeded = false;
        if (readLatched) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            readLatched = false;
        }

        while (!pathList.isEmpty()) {
            int pageId = pathList.getLastPageId();
            long parentLsn = pathList.getLastPageLsn();
            pathList.moveLast();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            node.acquireReadLatch();
            readLatched = true;
            try {
                interiorFrame.setPage(node);
                boolean isLeaf = interiorFrame.isLeaf();
                long pageLsn = interiorFrame.getPageLsn();

                if (pageId != rootPage && parentLsn < interiorFrame.getPageNsn()) {
                    // Concurrent split detected, we need to visit the right
                    // page
                    int rightPage = interiorFrame.getRightPage();
                    if (rightPage != -1) {
                        pathList.add(rightPage, parentLsn, -1);
                    }
                }

                if (!isLeaf) {
                    if (pageId == rootPage) {
                        rootLevel = interiorFrame.getLevel();
                    }
                    if (rootLevel - desiredLevelFromRoot == interiorFrame.getLevel()) {
                        page = node;
                        tupleIndex = 0;
                        succeeded = true;
                        return true;
                    }

                    for (int i = interiorFrame.getTupleCount() - 1; i >= 0; i--) {
                        int childPageId = interiorFrame.getChildPageId(i);
                        pathList.add(childPageId, pageLsn, -1);
                    }

                } else {
                   // break;
                }
            } finally {
                if (!succeeded) {
                    if (readLatched) {
                        node.releaseReadLatch();
                        readLatched = false;
                        bufferCache.unpin(node);
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (page == null) {
            return false;
        }

        if (tupleIndex == interiorFrame.getTupleCount()) {
            System.out.println("tuple count: " + interiorFrame.getTupleCount());
            if (!fetchNextPage()) {
                return false;
            }
        }

        do {
            if (tupleIndex < interiorFrame.getTupleCount()) {
                frameTuple.resetByTupleIndex(interiorFrame, tupleIndex);
                tupleIndexInc = tupleIndex + 1;
                return true;
            }
        } while (fetchNextPage());
        System.out.println("tuple count: " + interiorFrame.getTupleCount());
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        tupleIndex = tupleIndexInc;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (this.page != null) {
            this.page.releaseReadLatch();
            readLatched = false;
            bufferCache.unpin(this.page);
            pathList.clear();
        }

        pathList = ((RTreeCursorInitialState) initialState).getPathList();
        rootPage = ((RTreeCursorInitialState) initialState).getRootPage();

        pathList.add(this.rootPage, -1, -1);
        tupleIndex = 0;
        fetchNextPage();
    }

    @Override
    public void reset() throws HyracksDataException {
        close();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        throw new HyracksDataException("Updating tuples is not supported with this cursor.");
    }
}
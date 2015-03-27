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

package edu.uci.ics.hyracks.storage.am.common.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.*;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.IFIFOPageQueue;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractTreeIndex implements ITreeIndex {

    protected int rootPage = 1;

    protected final IBufferCache bufferCache;
    protected final IFileMapProvider fileMapProvider;
    protected final IMetaDataManager freePageManager;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    protected FileReference file;
    protected int fileId = -1;

    protected boolean isActive = false;
    private boolean wasActivated = false;
    private boolean fileOpen = false;

    protected int BULKLOAD_LEAF_START = 0;

    public AbstractTreeIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IMetaDataManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            FileReference file) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
        this.file = file;
    }

    public synchronized void create() throws HyracksDataException {
        create(false);
    }

    private synchronized void create(boolean appendOnly) throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        freePageManager.open(fileId);
        if (!appendOnly) {
            // regular or empty tree
            rootPage = 1;
            BULKLOAD_LEAF_START = 2;
        } else {
            // bulkload-only tree (used e.g. for HDFS). -1 is meta page, -2 is root page
            int numPages = bufferCache.getNumPagesOfFile(fileId);
            rootPage = numPages > 2 ? numPages - 2 : 0;
            BULKLOAD_LEAF_START = 0;
        }
        if (!appendOnly) {
            initEmptyTree();
            freePageManager.close();
            bufferCache.closeFile(fileId);
        } else {
            initVirtualMetaDataFrame();
            fileOpen = true;
            //initEmptyTree();
        }
    }

    private void initEmptyTree() throws HyracksDataException {
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.init(metaFrame, rootPage);

        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        try {
            frame.setPage(rootNode);
            frame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch(true);
            bufferCache.unpin(rootNode);
        }
    }

    private void initVirtualMetaDataFrame() throws HyracksDataException {
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.init(metaFrame);
    }

    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                if (!fileOpen) {
                    bufferCache.openFile(fileId);
                }
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
        freePageManager.open(fileId);
        if (!fileOpen) {
            if (freePageManager.getFirstMetadataPage() < 1) {
                // regular or empty tree
                rootPage = 1;
                BULKLOAD_LEAF_START = 2;
            } else {
                // bulkload-only tree (used e.g. for HDFS). -1 is meta page, -2 is root page
                int numPages = bufferCache.getNumPagesOfFile(fileId);
                rootPage = numPages > 2 ? numPages - 2 : 0;
                BULKLOAD_LEAF_START = 0;
            }
            fileOpen = true;
        }

        // TODO: Should probably have some way to check that the tree is physically consistent
        // or that the file we just opened actually is a tree

        isActive = true;
        wasActivated = true;
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActive && wasActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }
        if (isActive) {
            freePageManager.close();
            bufferCache.closeFile(fileId);
            fileOpen = false;
        }

        isActive = false;
    }

    public synchronized void destroy() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        if (fileId == -1) {
            return;
        }
        bufferCache.deleteFile(fileId, true);
        bufferCache.getIOManager().delete(file);
        fileId = -1;
    }

    public synchronized void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        initEmptyTree();
    }

    public boolean isEmptyTree(ITreeIndexFrame frame) throws HyracksDataException {
        if (rootPage == -1)
            return true;
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            if (frame.getLevel() == 0 && frame.getTupleCount() == 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public byte getTreeHeight(ITreeIndexFrame frame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            return frame.getLevel();
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public IMetaDataManager getMetaManager() {
        return freePageManager;
    }

    public int getRootPageId() {
        return rootPage;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public abstract class AbstractTreeIndexBulkLoader implements IIndexBulkLoader {
        protected final MultiComparator cmp;
        protected final int slotSize;
        protected final int leafMaxBytes;
        protected final int interiorMaxBytes;
        protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        protected final ITreeIndexMetaDataFrame metaFrame;
        protected final ITreeIndexTupleWriter tupleWriter;
        protected ITreeIndexFrame leafFrame;
        protected ITreeIndexFrame interiorFrame;
        // Immutable bulk loaders write their root page at page -2, as needed e.g. by append-only file systems such as HDFS. 
        // Since loading this tree relies on the root page actually being at that point, no further inserts into that tree are allowed.
        // Currently, this is not enforced.
        protected boolean releasedLatches;
        protected int virtualFileId = bufferCache.createMemFile();
        protected int virtualPageIncrement = 0;
        public boolean appendOnly = false;
        protected final IFIFOPageQueue queue;
        //TODO: this seems ugly architecture-wise
        private ICachedPage filterPage = null;

        public AbstractTreeIndexBulkLoader(float fillFactor, boolean appendOnly) throws TreeIndexException,
                HyracksDataException {
            //Initialize the tree 
            if (appendOnly) {
                create(appendOnly);
                this.appendOnly = appendOnly;
                activate();
            }

            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();
            metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();

            queue = bufferCache.createFIFOQueue();

            if (!appendOnly && !isEmptyTree(leafFrame)) {
                throw new TreeIndexException("Cannot bulk-load a non-empty tree.");
            }

            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.confiscatePage(BufferedFileHandle
                    .getDiskPageId(fileId, leafFrontier.pageId));
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);
            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
        }

        public abstract void add(ITupleReference tuple) throws IndexException, HyracksDataException;

        protected void handleException() throws HyracksDataException {
            // Unlatch and unpin pages that weren't in the queue to avoid leaking memory.
            for (NodeFrontier nodeFrontier : nodeFrontiers) {
                ICachedPage frontierPage = nodeFrontier.page;
                if (bufferCache.isVirtual(frontierPage)) {
                    frontierPage.releaseWriteLatch(false);
                    bufferCache.returnPage(frontierPage);
                    continue;
                }
            }
            releasedLatches = true;
        }

        @Override
        public void end() throws HyracksDataException {
            //move the root page to the first data page if necessary
            bufferCache.finishQueue();
            if (!appendOnly) {
                ICachedPage newRoot = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
                newRoot.acquireWriteLatch();
                //root will be the highest frontier
                NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
                ICachedPage oldRoot = bufferCache.pin(
                        BufferedFileHandle.getDiskPageId(fileId, lastNodeFrontier.pageId), false);
                oldRoot.acquireReadLatch();
                lastNodeFrontier.page = oldRoot;
                try {
                    System.arraycopy(lastNodeFrontier.page.getBuffer().array(), 0, newRoot.getBuffer().array(), 0,
                            lastNodeFrontier.page.getBuffer().capacity());
                } finally {
                    newRoot.releaseWriteLatch(true);
                    bufferCache.unpin(newRoot);
                    oldRoot.releaseReadLatch();
                    bufferCache.unpin(oldRoot);

                    // register old root as a free page
                    freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

                }
                if (!releasedLatches) {
                    for (int i = 0; i < nodeFrontiers.size(); i++) {
                        try {
                            nodeFrontiers.get(i).page.releaseWriteLatch(false);
                        } catch (IllegalMonitorStateException e) {
                            //ignore illegal monitor state exception
                        }
                    }
                }
            }
            else {
                if (!releasedLatches) {
                    for (int i = 0; i < nodeFrontiers.size(); i++) {
                        try {
                            nodeFrontiers.get(i).page.releaseWriteLatch(false);
                        } catch (IllegalMonitorStateException e) {
                            //ignore illegal monitor state exception
                        }
                    }
                }
            }
        }

        protected void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.page = bufferCache.confiscatePage(-1);
            frontier.pageId = -1;
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }

        public ITreeIndexFrame getLeafFrame() {
            return leafFrame;
        }

        public void setLeafFrame(ITreeIndexFrame leafFrame) {
            this.leafFrame = leafFrame;
        }

        public void insertFilterPage(ICachedPage page) {
            filterPage = page;
        }
    }

    public class TreeIndexInsertBulkLoader implements IIndexBulkLoader {
        ITreeIndexAccessor accessor;

        public TreeIndexInsertBulkLoader() throws HyracksDataException {
            accessor = (ITreeIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            try {
                accessor.insert(tuple);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void end() throws HyracksDataException {
            // do nothing
        }

    }

    @Override
    public long getMemoryAllocationSize() {
        return 0;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    @Override
    public boolean hasMemoryComponents() {
        return true;
    }
}

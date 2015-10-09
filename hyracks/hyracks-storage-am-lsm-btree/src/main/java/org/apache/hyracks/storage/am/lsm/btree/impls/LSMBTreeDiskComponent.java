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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;

public class LSMBTreeDiskComponent extends AbstractDiskLSMComponent {
    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMBTreeDiskComponent(BTree btree, BloomFilter bloomFilter, ILSMComponentFilter filter) {
        super(filter);
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
        bloomFilter.deactivate();
        bloomFilter.destroy();
    }

    public BTree getBTree() {
        return btree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public long getComponentSize() {
        IIOManager iomanager = btree.getBufferCache().getIOManager();
        long btreeSize, bloomSize = 0;
        try {
            IFileHandle btreeHandle = iomanager.open(btree.getFileReference(),
                                                     IIOManager.FileReadWriteMode.READ_ONLY,
                                                     IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);

            IFileHandle bloomHandle = iomanager.open(bloomFilter.getFileReference(),
                                                     IIOManager.FileReadWriteMode.READ_ONLY,
                                                     IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            btreeSize = iomanager.getSize(btreeHandle);
            bloomSize = iomanager.getSize(bloomHandle);
        } catch (HyracksDataException e) {
            btreeSize = -1;
        }
        return btreeSize + bloomSize ;
    }

    @Override
    public int getFileReferenceCount() {
        return btree.getBufferCache().getFileReferenceCount(btree.getFileId());
    }
}

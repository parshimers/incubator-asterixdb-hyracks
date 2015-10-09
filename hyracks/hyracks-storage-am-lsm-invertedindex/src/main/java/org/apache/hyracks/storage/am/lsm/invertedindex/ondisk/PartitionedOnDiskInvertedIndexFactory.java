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
package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.io.File;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexFileNameMapper;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class PartitionedOnDiskInvertedIndexFactory extends OnDiskInvertedIndexFactory {
    
    public PartitionedOnDiskInvertedIndexFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IInvertedListBuilderFactory invListBuilderFactory, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IInvertedIndexFileNameMapper fileNameMapper) {
        super(bufferCache, fileMapProvider, invListBuilderFactory, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, fileNameMapper);
    }

    @Override
    public IInvertedIndex createIndexInstance(FileReference dictBTreeFile) throws IndexException {
        String invListsFilePath = fileNameMapper.getInvListsFilePath(dictBTreeFile.getPath());
        FileReference invListsFile = new FileReference(invListsFilePath);
        IInvertedListBuilder invListBuilder = invListBuilderFactory.create();
        return new PartitionedOnDiskInvertedIndex(bufferCache, fileMapProvider, invListBuilder, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, dictBTreeFile, invListsFile);
    }
}

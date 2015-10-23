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

package org.apache.hyracks.storage.am.common.freepage;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.IMetadataManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LinkedListMetadataManagerFactory implements IMetadataManagerFactory {

    private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
    private final IBufferCache bufferCache;

    public LinkedListMetadataManagerFactory(IBufferCache bufferCache,
                                            ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        this.metaDataFrameFactory = metaDataFrameFactory;
        this.bufferCache = bufferCache;
    }

    public IMetaDataPageManager createFreePageManager() throws HyracksDataException {
        return new LinkedMetaDataPageManager(bufferCache, metaDataFrameFactory);
    }
}

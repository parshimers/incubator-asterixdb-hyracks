/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

/**
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan/merge/flush) on an {@link ILSMIndex}.
 * An {@link ILSMIndexAccessor} is not thread safe, but different {@link ILSMIndexAccessor}s
 * can concurrently operate on the same {@link ILSMIndex} (i.e., the {@link ILSMIndex} must allow
 * concurrent operations).
 */
public interface ILSMIndexAccessor extends IIndexAccessor {
    /**
     * Force a flush of the in-memory component.
     * 
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    public void flush() throws HyracksDataException, IndexException;

    /**
     * Merge all on-disk components.
     * 
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    public void merge() throws HyracksDataException, IndexException;
}

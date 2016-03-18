/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hyracks.dataflow.std.sort;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;

public class FrameSorterMergeSort extends AbstractFrameSorter {

    private int[] tPointersTemp;

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Integer.MAX_VALUE);
    }

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit) throws HyracksDataException {
        super(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                outputLimit);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        if (tPointersTemp == null || tPointersTemp.length < tPointers.length) {
            tPointersTemp = new int[tPointers.length];
        }
        sort(0, tupleCount);
    }

    @Override
    public void close() {
        super.close();
        tPointersTemp = null;
    }

    void sort(int offset, int length) throws HyracksDataException {
        int step = 1;
        int end = offset + length;
        /** bottom-up merge */
        while (step < length) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    System.arraycopy(tPointers, i * 4, tPointersTemp, i * 4, (end - i) * 4);
                }
            }
            /** prepare next phase merge */
            step *= 2;
            int[] tmp = tPointersTemp;
            tPointersTemp = tPointers;
            tPointers = tmp;
        }
    }

    /**
     * Merge two subarrays into one
     */
    private void merge(int start1, int start2, int len1, int len2) throws HyracksDataException {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(pos1, targetPos);
                pos1++;
            } else {
                copy(pos2, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            System.arraycopy(tPointers, pos1 * 4, tPointersTemp, targetPos * 4, rest * 4);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            System.arraycopy(tPointers, pos2 * 4, tPointersTemp, targetPos * 4, rest * 4);
        }
    }

    private void copy(int src, int dest) {
        tPointersTemp[dest * 4] = tPointers[src * 4];
        tPointersTemp[dest * 4 + 1] = tPointers[src * 4 + 1];
        tPointersTemp[dest * 4 + 2] = tPointers[src * 4 + 2];
        tPointersTemp[dest * 4 + 3] = tPointers[src * 4 + 3];
    }

}

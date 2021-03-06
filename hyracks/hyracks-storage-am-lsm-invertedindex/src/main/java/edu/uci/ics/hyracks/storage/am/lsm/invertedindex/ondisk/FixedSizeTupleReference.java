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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FixedSizeTupleReference implements ITupleReference {

    private final ITypeTraits[] typeTraits;
    private final int[] fieldStartOffsets;
    private byte[] data;
    private int startOff;

    public FixedSizeTupleReference(ITypeTraits[] typeTraits) {
        this.typeTraits = typeTraits;
        this.fieldStartOffsets = new int[typeTraits.length];
        this.fieldStartOffsets[0] = 0;
        for (int i = 1; i < typeTraits.length; i++) {
            fieldStartOffsets[i] = fieldStartOffsets[i - 1] + typeTraits[i - 1].getFixedLength();
        }
    }

    public void reset(byte[] data, int startOff) {
        this.data = data;
        this.startOff = startOff;
    }

    @Override
    public int getFieldCount() {
        return typeTraits.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return typeTraits[fIdx].getFixedLength();
    }

    @Override
    public int getFieldStart(int fIdx) {
        return startOff + fieldStartOffsets[fIdx];
    }
}

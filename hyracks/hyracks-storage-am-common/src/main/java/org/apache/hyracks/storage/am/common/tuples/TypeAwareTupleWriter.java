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

package org.apache.hyracks.storage.am.common.tuples;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    protected final ITypeTraits[] typeTraits;
    protected VarLenIntEncoderDecoder.VarLenIntDecoder decoder = VarLenIntEncoderDecoder.createDecoder();

    public TypeAwareTupleWriter(ITypeTraits[] typeTraits) {
        this.typeTraits = typeTraits;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int bytes = getNullFlagsBytes(tuple) + getFieldSlotsBytes(tuple);
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        int bytes = getNullFlagsBytes(numFields) + getFieldSlotsBytes(tuple, startField, numFields);
        for (int i = startField; i < startField + numFields; i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return new TypeAwareTupleReference(typeTraits);
    }

    @Override
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
        return writeTuple(tuple, targetBuf.array(), targetOff);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(tuple);
        // write null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }

        // write field slots for variable length fields
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(tuple.getFieldLength(i), targetBuf, runner);
            }
        }

        // write data fields
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            runner += tuple.getFieldLength(i);
        }

        return runner - targetOff;
    }

    @Override
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(numFields);
        // write null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }

        // write field slots for variable length fields
        for (int i = startField; i < startField + numFields; i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(tuple.getFieldLength(i), targetBuf, runner);
            }
        }

        for (int i = startField; i < startField + numFields; i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            runner += tuple.getFieldLength(i);
        }

        return runner - targetOff;
    }

    protected int getNullFlagsBytes(ITupleReference tuple) {
        return (int) Math.ceil(tuple.getFieldCount() / 8.0);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple) {
        int fieldSlotBytes = 0;
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            if (!typeTraits[i].isFixedLength()) {
                fieldSlotBytes += VarLenIntEncoderDecoder.getBytesRequired(tuple.getFieldLength(i));
            }
        }
        return fieldSlotBytes;
    }

    protected int getNullFlagsBytes(int numFields) {
        return (int) Math.ceil(numFields / 8.0);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple, int startField, int numFields) {
        int fieldSlotBytes = 0;
        for (int i = startField; i < startField + numFields; i++) {
            if (!typeTraits[i].isFixedLength()) {
                fieldSlotBytes += VarLenIntEncoderDecoder.getBytesRequired(tuple.getFieldLength(i));
            }
        }
        return fieldSlotBytes;
    }

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return bytesRequired(tuple);
    }
}

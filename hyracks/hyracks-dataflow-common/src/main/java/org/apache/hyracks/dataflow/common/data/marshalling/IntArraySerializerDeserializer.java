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
package org.apache.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IntArraySerializerDeserializer implements ISerializerDeserializer<int[]> {
    private static final long serialVersionUID = 1L;

    public static final IntArraySerializerDeserializer INSTANCE = new IntArraySerializerDeserializer();

    private IntArraySerializerDeserializer() {
    }

    @Override
    public int[] deserialize(DataInput in) throws HyracksDataException {
        try {
            int len = in.readInt();
            int[] array = new int[len];
            for (int i = 0; i < array.length; ++i) {
                array[i] = in.readInt();
            }
            return array;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(int[] instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.length);
            for (int i = 0; i < instance.length; ++i) {
                out.writeInt(instance[i]);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
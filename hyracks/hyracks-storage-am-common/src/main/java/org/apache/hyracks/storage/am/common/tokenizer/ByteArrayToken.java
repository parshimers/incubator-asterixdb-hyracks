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

package org.apache.hyracks.storage.am.common.tokenizer;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IToken;

public class ByteArrayToken implements IToken {
    
    private int length;
    private int tokenLength;
    private int start;
    private int tokenCount;
    private byte[] data;
    private final byte tokenTypeTag;

    public ByteArrayToken(byte tokenTypeTag) {
        this.tokenTypeTag = tokenTypeTag;
    }
    
    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getTokenLength() {
        return tokenLength;
    }

    @Override
    public void reset(byte[] data, int start, int length, int tokenLength, int tokenCount) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.tokenLength = tokenLength;
        this.tokenCount = tokenCount;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        if (tokenTypeTag > 0) {
            out.getDataOutput().write(tokenTypeTag);
        }
        out.getDataOutput().writeShort(length);
        ByteArraySerializerDeserializer.INSTANCE.serialize(data, start, length, out.getDataOutput());
    }

    @Override
    public void serializeTokenCount(GrowableArray out) throws IOException {
        return; //no op
    }
}

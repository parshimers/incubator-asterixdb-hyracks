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

package org.apache.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.util.bytes.Base64Parser;

public class ByteArrayBase64ParserFactory implements IValueParserFactory {

    private static final long serialVersionUID = 1L;
    public static final ByteArrayBase64ParserFactory INSTANCE = new ByteArrayBase64ParserFactory();

    private ByteArrayBase64ParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            Base64Parser parser = new Base64Parser();
            ByteArraySerializerDeserializer serializer = ByteArraySerializerDeserializer.INSTANCE;

            @Override
            public void parse(char[] input, int start, int length, DataOutput out) throws HyracksDataException {

                parser.generatePureByteArrayFromBase64String(input, start, length);
                try {
                    serializer.serialize(parser.getByteArray(), 0, parser.getLength(), out);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}

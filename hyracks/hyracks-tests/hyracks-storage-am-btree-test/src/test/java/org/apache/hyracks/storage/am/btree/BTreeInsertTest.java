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

package org.apache.hyracks.storage.am.btree;

import java.util.Random;

import org.junit.After;
import org.junit.Before;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.util.BTreeTestContext;
import org.apache.hyracks.storage.am.btree.util.BTreeTestHarness;

/**
 * Tests the BTree insert operation with strings and integer fields using 
 * various numbers of key and payload fields. Each tests first fills a BTree with 
 * randomly generated tuples. We compare the following operations against expected results: 
 *      1) Point searches for all tuples 
 *      2) Ordered scan
 *      3) Disk-order scan
 *      4) Range search (and prefix search for composite keys)
 */
public class BTreeInsertTest extends OrderedIndexInsertTest {

    private final BTreeTestHarness harness = new BTreeTestHarness();

    public BTreeInsertTest() {
        super(BTreeTestHarness.LEAF_FRAMES_TO_TEST);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType) throws Exception {
        return BTreeTestContext.create(harness.getBufferCache(), harness.getFileMapProvider(),
                harness.getFileReference(), fieldSerdes, numKeys, leafType);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}

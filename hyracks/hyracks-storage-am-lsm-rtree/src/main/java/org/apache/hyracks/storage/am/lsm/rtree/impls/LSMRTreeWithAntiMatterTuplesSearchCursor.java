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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;

public class LSMRTreeWithAntiMatterTuplesSearchCursor extends LSMIndexSearchCursor {

    private ITreeIndexAccessor[] mutableRTreeAccessors;
    private ITreeIndexAccessor[] btreeAccessors;
    private RTreeSearchCursor[] mutableRTreeCursors;
    private ITreeIndexCursor[] btreeCursors;
    private RangePredicate btreeRangePredicate;
    private boolean foundNext;
    private ITupleReference frameTuple;
    private int[] comparatorFields;
    private MultiComparator btreeCmp;
    private int currentCursor;
    private SearchPredicate rtreeSearchPredicate;
    private int numMutableComponents;
    private boolean open;

    private boolean useProceedResult = false;
    private RecordDescriptor rDescForProceedReturnResult = null;
    private byte[] valuesForOperationCallbackProceedReturnResult;
    private boolean resultOfsearchCallBackProceed = false;
    private int numberOfFieldFromIndex = 0;
    private ArrayTupleBuilder tupleBuilderForProceedResult;
    private ArrayTupleReference copyTuple = null;
    protected ISearchOperationCallback searchCallback;

    // For the experiment
    protected int proceedFailCount = 0;
    protected int proceedSuccessCount = 0;
    private static final Logger LOGGER = Logger.getLogger(LSMRTreeSearchCursor.class.getName());
    private static final Level LVL = Level.FINEST;
    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false);
    }

    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        super(opCtx, returnDeletedTuples);
        currentCursor = 0;
        this.useProceedResult = opCtx.getUseOperationCallbackProceedReturnResult();
        this.rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
        this.valuesForOperationCallbackProceedReturnResult = opCtx.getValuesForProceedReturnResult();
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getHilbertCmp();
        btreeCmp = lsmInitialState.getBTreeCmp();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        comparatorFields = lsmInitialState.getComparatorFields();
        operationalComponents = lsmInitialState.getOperationalComponents();
        rtreeSearchPredicate = (SearchPredicate) searchPred;
        //a buddy-btree entry consists of key and value fields
        numberOfFieldFromIndex = btreeCmp.getKeyFieldCount(); 

        // If it is required to use the result of searchCallback.proceed(),
        // we need to initialize the byte array that contains true and false result.
        //
        if (useProceedResult) {
            //            returnValuesArrayForProccedResult =
            //            byte[] tmpResultArray = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
            //            rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
            //            ISerializerDeserializer<Object> serializerDeserializerForProceedReturnResult = rDescForProceedReturnResult
            //                    .getFields()[rDescForProceedReturnResult.getFieldCount() - 1];
            //            // INT is 4 byte, however since there is a tag before the actual value,
            //            // we need to provide 5 byte. The serializer is already chosen so the typetag can be anything.
            //            ByteArrayInputStream inStreamZero = new ByteArrayInputStream(tmpResultArray, 0, 5);
            //            ByteArrayInputStream inStreamOne = new ByteArrayInputStream(tmpResultArray, 5, 5);
            //            Object AInt32Zero = serializerDeserializerForProceedReturnResult.deserialize(new DataInputStream(
            //                    inStreamZero));
            //            Object AInt32One = serializerDeserializerForProceedReturnResult
            //                    .deserialize(new DataInputStream(inStreamOne));
            //            ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
            //            serializerDeserializerForProceedReturnResult.serialize(AInt32Zero, castBuffer.getDataOutput());
            //            System.arraycopy(castBuffer.getByteArray(), 0, returnValuesArrayForProccedResult, 0, castBuffer.getLength());
            //            castBuffer.reset();
            //            serializerDeserializerForProceedReturnResult.serialize(AInt32One, castBuffer.getDataOutput());
            //            System.arraycopy(castBuffer.getByteArray(), 0, returnValuesArrayForProccedResult, 5, castBuffer.getLength());

            tupleBuilderForProceedResult = new ArrayTupleBuilder(numberOfFieldFromIndex + 1);
            copyTuple = new ArrayTupleReference();
            
        }
        
        includeMutableComponent = false;
        numMutableComponents = 0;
        int numImmutableComponents = 0;
        for (ILSMComponent component : operationalComponents) {
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                numMutableComponents++;
            } else {
                numImmutableComponents++;
            }
        }
        if (includeMutableComponent) {
            btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
        }

        mutableRTreeCursors = new RTreeSearchCursor[numMutableComponents];
        mutableRTreeAccessors = new ITreeIndexAccessor[numMutableComponents];
        btreeCursors = new BTreeRangeSearchCursor[numMutableComponents];
        btreeAccessors = new ITreeIndexAccessor[numMutableComponents];
        for (int i = 0; i < numMutableComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree = (RTree) ((LSMRTreeMemoryComponent) component).getRTree();
            BTree btree = (BTree) ((LSMRTreeMemoryComponent) component).getBTree();
            mutableRTreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory()
                    .createFrame(), false);
            btreeAccessors[i] = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            mutableRTreeAccessors[i] = rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
        }

        rangeCursors = new RTreeSearchCursor[numImmutableComponents];
        ITreeIndexAccessor[] immutableRTreeAccessors = new ITreeIndexAccessor[numImmutableComponents];
        int j = 0;
        for (int i = numMutableComponents; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            rangeCursors[j] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
            RTree rtree = (RTree) ((LSMRTreeDiskComponent) component).getRTree();
            immutableRTreeAccessors[j] = rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            immutableRTreeAccessors[j].search(rangeCursors[j], searchPred);
            j++;
        }
        searchNextCursor();
        setPriorityQueueComparator();
        initPriorityQueue();
        open = true;
    }

    private void searchNextCursor() throws HyracksDataException, IndexException {
        if (currentCursor < numMutableComponents) {
            mutableRTreeCursors[currentCursor].reset();
            mutableRTreeAccessors[currentCursor].search(mutableRTreeCursors[currentCursor], rtreeSearchPredicate);
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (includeMutableComponent) {
            if (foundNext) {
                return true;
            }

            while (currentCursor < numMutableComponents) {
                while (mutableRTreeCursors[currentCursor].hasNext()) {
                    mutableRTreeCursors[currentCursor].next();
                    ITupleReference currentTuple = mutableRTreeCursors[currentCursor].getTuple();
                    
                    // TODO: at this point, we only add proceed() and cancelProceed() part.
                    // reconcile() and complete() can be added later after considering the semantics.

                    // Call proceed() to do necessary operations before returning this tuple.
                    resultOfsearchCallBackProceed = searchCallback.proceed(currentTuple);
                    if (searchMemBTrees(currentTuple, currentCursor)) {
                        //anti-matter tuple is NOT found
                        foundNext = true;
                        frameTuple = currentTuple;
                        return true;
                    } else {
                        //anti-matter tuple is found
                        // need to reverse the effect of proceed() since we can't return this tuple.
                        if (resultOfsearchCallBackProceed) {
                            searchCallback.cancelProceed(currentTuple);
                        }
                    }
                }
                mutableRTreeCursors[currentCursor].close();
                currentCursor++;
                searchNextCursor();
            }
            while (super.hasNext()) {
                super.next();
                ITupleReference diskRTreeTuple = super.getTuple();
                // TODO: at this point, we only add proceed() and cancelProceed() part.
                // reconcile() and complete() can be added later after considering the semantics.

                // Call proceed() to do necessary operations before returning this tuple.
                resultOfsearchCallBackProceed = searchCallback.proceed(diskRTreeTuple);
                if (searchMemBTrees(diskRTreeTuple, numMutableComponents)) {
                    //anti-matter tuple is NOT found
                    foundNext = true;
                    frameTuple = diskRTreeTuple;
                    return true;
                }   else {
                    //anti-matter tuple is found
                    // need to reverse the effect of proceed() since we can't return this tuple.
                    if (resultOfsearchCallBackProceed) {
                        searchCallback.cancelProceed(diskRTreeTuple);
                    }
                }
            }
        } else {
            if (super.hasNext()) {
                super.next();
                ITupleReference diskRTreeTuple = super.getTuple();

                // TODO: at this point, we only add proceed() and cancelProceed() part.
                // reconcile() and complete() can be added later after considering the semantics.

                // Call proceed() to do necessary operations before returning this tuple.
                resultOfsearchCallBackProceed = searchCallback.proceed(diskRTreeTuple);
                foundNext = true;
                frameTuple = diskRTreeTuple;
                return true;
            }
        }

        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;
        
        //  If useProceed is set to true (e.g., in case of an index-only plan)
        //  and searchCallback.proceed() fail: will add zero - default value
        //                            success: will add one - default value
        if (useProceedResult) {
            tupleBuilderForProceedResult.reset();
            TupleUtils.copyTuple(tupleBuilderForProceedResult, frameTuple, numberOfFieldFromIndex);

            if (!resultOfsearchCallBackProceed) {
                // fail case - add the value that indicates fail.
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 0, 5);

                // For the experiment
                proceedFailCount += 1;
            } else {
                // success case - add the value that indicates success.
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 5, 5);

                // For the experiment
                proceedSuccessCount += 1;
            }
            copyTuple.reset(tupleBuilderForProceedResult.getFieldEndOffsets(),
                    tupleBuilderForProceedResult.getByteArray());
            frameTuple = copyTuple;
        }
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        if (!open) {
            return;
        }
        currentCursor = 0;
        foundNext = false;
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].reset();
                btreeCursors[i].reset();
            }
        }
        super.reset();

        // For the experiment
        proceedFailCount = 0;
        proceedSuccessCount = 0;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].close();
                btreeCursors[i].close();
            }
        }
        currentCursor = 0;
        open = false;
        super.close();
        // For the experiment
        if (useProceedResult) {
            LOGGER.log(LVL, "***** [Index-only experiment] RTREE-SEARCH tryLock count\tS:\t" + proceedSuccessCount
                    + "\tF:\t" + proceedFailCount);
        }
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        return cmp.selectiveFieldCompare(tupleA, tupleB, comparatorFields);
    }

    private boolean searchMemBTrees(ITupleReference tuple, int lastBTreeToSearch) throws HyracksDataException,
            IndexException {
        for (int i = 0; i < lastBTreeToSearch; i++) {
            btreeCursors[i].reset();
            btreeRangePredicate.setHighKey(tuple, true);
            btreeRangePredicate.setLowKey(tuple, true);
            btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
            try {
                if (btreeCursors[i].hasNext()) {
                    return false;
                }
            } finally {
                btreeCursors[i].close();
            }
        }
        return true;
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueHilbertComparator(cmp, comparatorFields);
        }
    }

    public class PriorityQueueHilbertComparator extends PriorityQueueComparator {

        private final int[] comparatorFields;

        public PriorityQueueHilbertComparator(MultiComparator cmp, int[] comparatorFields) {
            super(cmp);
            this.comparatorFields = comparatorFields;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result;
            try {
                result = cmp.selectiveFieldCompare(elementA.getTuple(), elementB.getTuple(), comparatorFields);
                if (result != 0) {
                    return result;
                }
            } catch (HyracksDataException e) {
                throw new IllegalArgumentException(e);
            }

            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}

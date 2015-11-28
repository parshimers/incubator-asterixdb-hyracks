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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ILinearizerSearchHelper;
import org.apache.hyracks.storage.am.common.api.ILinearizerSearchPredicate;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class HilbertValueBTreeRangeSearchCursor implements ITreeIndexCursor {

    private static final boolean DEBUG = false;

    private final static int HILBERT_SPACE_ORDER = 31;
    private static final int MAX_COORDINATE = 180;
    private static final long COORDINATE_EXTENSION_FACTOR = (long) (((double) (1L << HILBERT_SPACE_ORDER)) / (2 * MAX_COORDINATE));
    private static final int MAX_TREE_LEVEL = HILBERT_SPACE_ORDER;
    private static final int DIMENSION = 2; //Only two dimensional data are supported.
    private final IBTreeLeafFrame frame;
    private final boolean exclusiveLatchNodes;
    private ICursorInitialState cursorInitialiState;
    private BTreeRangeSearchCursor cursor;
    private boolean hasNext;
    private SearchSpaceContext currentSearchCtx;
    private SearchSpaceContext backtrackSearchCtx;
    private SearchSpaceContext tempSearchCtx;
    private int prevPageId;

    private MultiComparator lowKeyComparator; //not needed for now. 
    private ITupleReference tRefNextMatch;
    private ArrayTupleBuilder tBuilderNextMatch;
    private double searchedPoint[] = new double[DIMENSION];
    private long searchedLongCoordinate[] = new long[DIMENSION];

    private boolean firstOpen;
    private IIndexAccessor btreeAccessor;

    private ILinearizerSearchPredicate linearizerSearchPredicate;
    private ILinearizerSearchHelper linearizerSearchHelper;

    private boolean isPointQuery;
    private int pointQueryNextMatchCallCount;

    private final long wholeSearchSpaceBL[] = new long[] { 0, 0 };
    private final long wholeSearchSpaceTR[] = new long[] { 1L << MAX_TREE_LEVEL, 1L << MAX_TREE_LEVEL };
    private long wholeQueryRegionBL[] = new long[DIMENSION];
    private long wholeQueryRegionTR[] = new long[DIMENSION];
    boolean hasBacktrack;
    boolean considerBacktrack;
    private long pageKey;
    private long prevHilbertValue;
    private long curHilbertValue;

    private final HilbertState[] states = new HilbertState[] {
            new HilbertState(new int[] { 3, 0, 0, 1 }, new int[] { 0, 1, 3, 2 }, new int[] { 0, 1, 3, 2 }),
            new HilbertState(new int[] { 2, 1, 1, 0 }, new int[] { 2, 1, 3, 0 }, new int[] { 3, 1, 0, 2 }),
            new HilbertState(new int[] { 1, 2, 2, 3 }, new int[] { 2, 3, 1, 0 }, new int[] { 3, 2, 0, 1 }),
            new HilbertState(new int[] { 0, 3, 3, 2 }, new int[] { 0, 3, 1, 2 }, new int[] { 0, 2, 3, 1 }) };

    public HilbertValueBTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this.frame = frame;
        this.exclusiveLatchNodes = exclusiveLatchNodes;
        this.currentSearchCtx = new SearchSpaceContext();
        tBuilderNextMatch = new ArrayTupleBuilder(1);
        firstOpen = true;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        //TODO
        //make a better design to avoid this style of recursive call termination   
        if (!firstOpen) {
            this.cursorInitialiState = initialState;
            btreeAccessor = ((BTreeCursorInitialState) initialState).getAccessor();
            //            if (!(searchPred instanceof ILinearizerSearchPredicate)) {
            //                tRefNextMatch = ((RangePredicate) searchPred).getLowKey();
            //                ((RangePredicate) linearizerSearchPredicate).setLowKey(tRefNextMatch, true);
            //                cursor = new BTreeRangeSearchCursor(frame, exclusiveLatchNodes);
            //                cursor.open(cursorInitialiState, linearizerSearchPredicate);
            //            }
            return;
        }

        this.cursorInitialiState = initialState;
        btreeAccessor = ((BTreeCursorInitialState) initialState).getAccessor();
        lowKeyComparator = searchPred.getLowKeyComparator();
        tRefNextMatch = ((RangePredicate) searchPred).getLowKey();

        linearizerSearchPredicate = (ILinearizerSearchPredicate) searchPred;
        linearizerSearchHelper = linearizerSearchPredicate.getLinearizerSearchHelper();

        //Warning: caller must make sure that all coordinates are in geo-space. 
        wholeQueryRegionBL[0] = getLongCoordinate(linearizerSearchHelper.getQueryBottomLeftX());
        wholeQueryRegionBL[1] = getLongCoordinate(linearizerSearchHelper.getQueryBottomLeftY());
        wholeQueryRegionTR[0] = getLongCoordinate(linearizerSearchHelper.getQueryTopRightX());
        wholeQueryRegionTR[1] = getLongCoordinate(linearizerSearchHelper.getQueryTopRightY());

        if (firstOpen) {
            if (wholeQueryRegionBL[0] == wholeQueryRegionTR[0] && wholeQueryRegionBL[1] == wholeQueryRegionTR[1]) {
                isPointQuery = true;
                pointQueryNextMatchCallCount = 0;
            } else {
                isPointQuery = false;
            }
        }

        pageKey = 0;
        cursor = calculateNextMatch(false);
        if (cursor != null) {
            prevPageId = cursor.getPageId();
        }
        hasNext = false;
        firstOpen = false;
    }

    private long getLongCoordinate(double c) {
        return ((long) ((c + (double) MAX_COORDINATE) * (double) COORDINATE_EXTENSION_FACTOR));
    }

    @Override
    public void close() throws HyracksDataException {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (hasNext) {
            return true;
        }
        while (true) {
            if (cursor == null) {
                return false;
            }
            if (!cursor.hasNext()) {
                cursor.close();
                cursor = null;
                return false;
            }
            if (prevPageId == cursor.getPageId()) {
                cursor.next();
                if (isPointOnQueryRegion(cursor.getTuple())) {
                    hasNext = true;
                    return true;
                }

                //Consume all points which are located in the same coordinates.
                //This consumption is a critical operation in order to make the current pageKey set to 
                //the next distinct point. If this consumption isn't executed, region query may terminate 
                //earlier without searching through all overlapped segments of the region correctly.
                prevHilbertValue = linearizerSearchHelper.convertInt64Field2Long(cursor.getTuple().getFieldData(0),
                        cursor.getTuple().getFieldStart(0));

                while (cursor.hasNext()) {
                    cursor.next();
                    curHilbertValue = linearizerSearchHelper.convertInt64Field2Long(cursor.getTuple().getFieldData(0),
                            cursor.getTuple().getFieldStart(0));
                    if (prevHilbertValue != curHilbertValue) {
                        break;
                    }
                }
                if (isPointOnQueryRegion(cursor.getTuple())) {
                    prevPageId = cursor.getPageId();
                    hasNext = true;
                    return true;
                }
            } else {
                cursor.next();
                if (isPointOnQueryRegion(cursor.getTuple())) {
                    //If the first point in the next leaf page is on the query region, 
                    //continue checking all points in the next page.
                    prevPageId = cursor.getPageId();
                    hasNext = true;
                    return true;
                }
                //The next leaf page is read and the first record of the next page is set as the current page key.
                //Then calculate the next match.
                currentSearchCtx.init();
                pageKey = linearizerSearchHelper.convertInt64Field2Long(cursor.getTuple().getFieldData(0), cursor
                        .getTuple().getFieldStart(0));
                cursor.close();
                cursor = null;
                cursor = calculateNextMatch(true);
                if (cursor != null) {
                    prevPageId = cursor.getPageId();
                }
            }
        }
    }

    @Override
    public ICachedPage getPage() {
        if (cursor != null) {
            return cursor.getPage();
        }
        return null;
    }

    @Override
    public void next() throws HyracksDataException {
        hasNext = false;
    }

    @Override
    public void reset() throws HyracksDataException {
        close();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        if (cursor != null) {
            cursor.setBufferCache(bufferCache);
        }
    }

    @Override
    public void setFileId(int fileId) {
        if (cursor != null) {
            cursor.setFileId(fileId);
        }
    }

    @Override
    public boolean exclusiveLatchNodes() {
        if (cursor != null) {
            return cursor.exclusiveLatchNodes();
        }
        return false;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        if (cursor != null) {
            cursor.markCurrentTupleAsUpdated();
        }
    }

    @Override
    public ITupleReference getTuple() {
        if (cursor != null) {
            return cursor.getTuple();
        }
        return null;
    }

    public int getTupleOffset() {
        if (cursor != null) {
            return cursor.getTupleOffset();
        }
        return -1;
    }

    public int getPageId() {
        if (cursor != null) {
            return cursor.getPageId();
        }
        return -1;
    }

    /* 
     * This method works according to the algorithms (Algorithm 6.4.1, Algorithm 6.4.2, and Algorithm 6.4.3)
     * described in the Ph.D. thesis whose title is 
     * "The Application of Space Filling Curves to the Storage and Retrieval of Multi-dimensional Data".
     */
    private BTreeRangeSearchCursor calculateNextMatch(boolean search) throws HyracksDataException, IndexException {
        boolean success;
        considerBacktrack = true;
        hasBacktrack = false;
        currentSearchCtx.init();

        if (DEBUG) {
            System.out.println("pageKey: " + pageKey + "=>" + Long.toBinaryString(pageKey));
        }

        while (currentSearchCtx.treeLevel <= MAX_TREE_LEVEL) {
            if (DEBUG) {
                currentSearchCtx.prettyPrint();
            }
            currentSearchCtx.computeQLower();
            currentSearchCtx.computeQUpper();
            if (considerBacktrack) {
                currentSearchCtx.computeH();
            }

            currentSearchCtx.resetDerivedKeys();
            success = binarySearchOverlappingQuadrants();
            if (!success) {
                return null;
            }

            if (DEBUG) {
                currentSearchCtx.prettyPrint();
            }

            if (currentSearchCtx.treeLevel != MAX_TREE_LEVEL) {
                currentSearchCtx.reduceSearchSpace();
            }

            if (considerBacktrack && currentSearchCtx.H == currentSearchCtx.minDerivedKey
                    && currentSearchCtx.areQueryRegionAndSearchSpaceEqual()) {
                /* special case: page-key is a next-match */
                currentSearchCtx.nextMatch = pageKey;
                break;
            }

            currentSearchCtx.updateNextMatch();
            currentSearchCtx.updateState();
            ++currentSearchCtx.treeLevel;
            if (considerBacktrack && currentSearchCtx.H < currentSearchCtx.minDerivedKey) {
                considerBacktrack = false;
            }

            if (!considerBacktrack && currentSearchCtx.treeLevel <= MAX_TREE_LEVEL
                    && currentSearchCtx.areQueryRegionAndSearchSpaceEqual()) {
                break;
            }
        }

        if (DEBUG) {
            currentSearchCtx.prettyPrint();
        }

        //prepare cursor to return
        setNextMatch(currentSearchCtx.nextMatch);
        cursor = new BTreeRangeSearchCursor(frame, exclusiveLatchNodes);
        if (search) {
            btreeAccessor.search(this, linearizerSearchPredicate);
        }
        cursor.open(cursorInitialiState, linearizerSearchPredicate);
        return cursor;
    }

    private void setNextMatch(long nextMatch) throws HyracksDataException {
        //revisit pointQuery handling
        if (isPointQuery && pointQueryNextMatchCallCount == 0) {
            pointQueryNextMatchCallCount = 1;
        }

        linearizerSearchHelper.convertLong2Int64Field(nextMatch, tBuilderNextMatch);
        ((ArrayTupleReference) tRefNextMatch).reset(tBuilderNextMatch.getFieldEndOffsets(),
                tBuilderNextMatch.getByteArray());
    }

    private void createBacktrackContext(int iterationCount, int cutDimension, boolean takeHigherCoordinates) {
        if (backtrackSearchCtx == null) {
            backtrackSearchCtx = new SearchSpaceContext();
        }

        for (int i = 0; i < DIMENSION; i++) {
            backtrackSearchCtx.searchSpaceBL[i] = currentSearchCtx.searchSpaceBL[i];
            backtrackSearchCtx.searchSpaceTR[i] = currentSearchCtx.searchSpaceTR[i];
            backtrackSearchCtx.queryRegionBL[i] = currentSearchCtx.queryRegionBL[i];
            backtrackSearchCtx.queryRegionTR[i] = currentSearchCtx.queryRegionTR[i];
        }

        backtrackSearchCtx.state = currentSearchCtx.state;
        backtrackSearchCtx.treeLevel = currentSearchCtx.treeLevel;
        backtrackSearchCtx.minDerivedKey = currentSearchCtx.minDerivedKey;
        backtrackSearchCtx.maxDerivedKey = currentSearchCtx.maxDerivedKey;
        backtrackSearchCtx.H = currentSearchCtx.H;
        backtrackSearchCtx.nextMatch = currentSearchCtx.nextMatch;
        backtrackSearchCtx.qLower = currentSearchCtx.qLower;
        backtrackSearchCtx.qUpper = currentSearchCtx.qUpper;
        backtrackSearchCtx.binarySearchIterationCount = iterationCount;
        backtrackSearchCtx.cutDimension = cutDimension;
        backtrackSearchCtx.takeHigherCoordinates = takeHigherCoordinates;
    }

    private void restoreBacktrackContext() {
        //adjust search space and query region and associated info accordingly.
        if (backtrackSearchCtx.binarySearchIterationCount == 1) {
            backtrackSearchCtx.reduceSearchSpace();
            backtrackSearchCtx.updateDerivedKeysForUpperHalf();
        }

        //swap two contexts
        tempSearchCtx = currentSearchCtx;
        currentSearchCtx = backtrackSearchCtx;
        backtrackSearchCtx = tempSearchCtx;

        //ignore backtrack from here
        considerBacktrack = false;
        hasBacktrack = false;
    }

    /* 
     * this method implements a binary search algorithm, Algorithm 6.4.3 in the Ph.D. thesis whose title is  
     * "The Application of Space Filling Curves to the Storage and Retrieval of Multi-dimensional Data".
     */
    private boolean binarySearchOverlappingQuadrants() {

        // coordinates
        //                |
        //                |
        //       01       |       11
        //                |
        //                |
        // --------------   ---------------
        //                |
        //                |
        //       00       |       10
        //                |
        //                |

        int maxLowerDerivedKey = (currentSearchCtx.minDerivedKey + currentSearchCtx.maxDerivedKey) / 2;
        int minHigherDerivedKey = maxLowerDerivedKey + 1;
        int maxLowerCoordinate = states[currentSearchCtx.state].coord[maxLowerDerivedKey];
        int minHigherCoordinate = states[currentSearchCtx.state].coord[minHigherDerivedKey];
        int partitioningDimensionBitMask = maxLowerCoordinate ^ minHigherCoordinate;
        int lowerHalfQuadrantsBitValue = maxLowerCoordinate & partitioningDimensionBitMask;
        boolean isLowerHalfOverlapped = false;
        boolean isUpperHalfOverlapped = false;
        int cutDimension;
        boolean takeHigherCoordinates;

        if ((currentSearchCtx.qLower & partitioningDimensionBitMask) == lowerHalfQuadrantsBitValue) {
            isLowerHalfOverlapped = true;
        } else {
            isUpperHalfOverlapped = true;
        }

        if ((currentSearchCtx.qUpper & partitioningDimensionBitMask) == lowerHalfQuadrantsBitValue) {
            isLowerHalfOverlapped = true;
        } else {
            isUpperHalfOverlapped = true;
        }

        cutDimension = partitioningDimensionBitMask == 1 ? 0 : 1;
        takeHigherCoordinates = minHigherCoordinate > maxLowerCoordinate;
        currentSearchCtx.binarySearchIterationCount = (currentSearchCtx.binarySearchIterationCount + 1) % 2;

        if (isLowerHalfOverlapped && (!considerBacktrack || maxLowerDerivedKey >= currentSearchCtx.H)) {
            if (isUpperHalfOverlapped && considerBacktrack) {
                createBacktrackContext(currentSearchCtx.binarySearchIterationCount, cutDimension, takeHigherCoordinates);
                hasBacktrack = true;
            }

            //prepare the next search space reduction
            currentSearchCtx.cutDimension = cutDimension;
            currentSearchCtx.takeHigherCoordinates = !takeHigherCoordinates;

            //either continue or return according to the iteration count
            if (currentSearchCtx.binarySearchIterationCount == 0) {
                currentSearchCtx.minDerivedKey = currentSearchCtx.minDerivedKey;
                return true;
            } else {
                //go to second iteration of binary search
                currentSearchCtx.reduceSearchSpace();
                currentSearchCtx.computeQLower();
                currentSearchCtx.computeQUpper();
                currentSearchCtx.updateDerivedKeysForLowerHalf();
                return binarySearchOverlappingQuadrants();
            }
        } else {
            if (isUpperHalfOverlapped) {
                //reduce current search space
                currentSearchCtx.cutDimension = cutDimension;
                currentSearchCtx.takeHigherCoordinates = takeHigherCoordinates;

                if (currentSearchCtx.binarySearchIterationCount == 0) {
                    currentSearchCtx.minDerivedKey = currentSearchCtx.maxDerivedKey;
                    return true;
                } else {
                    currentSearchCtx.reduceSearchSpace();
                    currentSearchCtx.computeQLower();
                    currentSearchCtx.computeQUpper();
                    currentSearchCtx.updateDerivedKeysForUpperHalf();
                    return binarySearchOverlappingQuadrants();
                }
            } else {
                if (hasBacktrack) {
                    restoreBacktrackContext();
                    if (currentSearchCtx.binarySearchIterationCount == 0) {
                        currentSearchCtx.minDerivedKey = currentSearchCtx.maxDerivedKey;
                        return true;
                    } else {
                        return binarySearchOverlappingQuadrants();
                    }
                } else {
                    return false;
                }
            }
        }
    }

    private boolean isPointOnQueryRegion(ITupleReference tuple) throws HyracksDataException {
        //An entry of HilbertValueBTreeIndex consists of [ Hilbert value (AINT64) | point (APOINT) | PK ].
        linearizerSearchHelper.convertPointField2TwoDoubles(tuple.getFieldData(0), tuple.getFieldStart(1),
                searchedPoint);
        searchedLongCoordinate[0] = getLongCoordinate(searchedPoint[0]);
        searchedLongCoordinate[1] = getLongCoordinate(searchedPoint[1]);

        if (DEBUG) {
            if (wholeQueryRegionBL[0] <= searchedLongCoordinate[0]
                    && wholeQueryRegionTR[0] >= searchedLongCoordinate[0]
                    && wholeQueryRegionBL[1] <= searchedLongCoordinate[1]
                    && wholeQueryRegionTR[1] >= searchedLongCoordinate[1]) {
                System.out.println("y: " + searchedPoint[0] + ", " + searchedPoint[1] + ", "
                        + searchedLongCoordinate[0] + ", " + searchedLongCoordinate[1] + ", "
                        + linearizerSearchHelper.convertInt64Field2Long(tuple.getFieldData(0), tuple.getFieldStart(0)));
            } else {
                System.out.println("n: " + searchedPoint[0] + ", " + searchedPoint[1] + ", "
                        + searchedLongCoordinate[0] + ", " + searchedLongCoordinate[1] + ", "
                        + linearizerSearchHelper.convertInt64Field2Long(tuple.getFieldData(0), tuple.getFieldStart(0)));
            }
        }
        return wholeQueryRegionBL[0] <= searchedLongCoordinate[0] && wholeQueryRegionTR[0] >= searchedLongCoordinate[0]
                && wholeQueryRegionBL[1] <= searchedLongCoordinate[1]
                && wholeQueryRegionTR[1] >= searchedLongCoordinate[1];
    }

    private class SearchSpaceContext {
        /*
         * this context contains all necessary information regarding a current-search-space.
         */

        //Bottom left and top right of x/y coordinates of current-search-space
        public long searchSpaceBL[] = new long[DIMENSION];
        public long searchSpaceTR[] = new long[DIMENSION];

        //Bottom left and top right of x/y coordinates of current-search-space
        public long queryRegionBL[] = new long[DIMENSION];
        public long queryRegionTR[] = new long[DIMENSION];
        public int qLower;
        public int qUpper;

        public int state;
        public int treeLevel;
        public int minDerivedKey;
        public int maxDerivedKey;
        public int H; /* pageKey's 2bits corresponding to the current tree-level */
        public long nextMatch;
        public int binarySearchIterationCount;
        public int cutDimension;
        public boolean takeHigherCoordinates;

        public void prettyPrint() {
            StringBuilder sb = new StringBuilder();
            sb.append("ss: " + searchSpaceBL[0] + "," + searchSpaceBL[1] + "," + searchSpaceTR[0] + ","
                    + searchSpaceTR[1]);
            sb.append(", ");
            sb.append("qr: " + queryRegionBL[0] + "," + queryRegionBL[1] + "," + queryRegionTR[0] + ","
                    + queryRegionTR[1]);
            sb.append(", ");
            sb.append("qL: " + qLower + ", qU: " + qUpper);
            sb.append(", ");
            sb.append("state: " + state);
            sb.append(", ");
            sb.append("minK: " + ((minDerivedKey <= 1) ? "0" : "") + Integer.toBinaryString(minDerivedKey) + ", maxK: "
                    + ((maxDerivedKey <= 1) ? "0" : "") + Integer.toBinaryString(maxDerivedKey) + ", H: "
                    + ((H <= 1) ? "0" : "") + Integer.toBinaryString(H));
            sb.append(", ");
            sb.append("match: " + nextMatch + "=>" + Long.toBinaryString(nextMatch));
            sb.append(", ");
            sb.append("level: " + treeLevel);
            System.out.println(sb.toString());
        }

        public void init() {
            for (int i = 0; i < DIMENSION; i++) {
                searchSpaceBL[i] = wholeSearchSpaceBL[i];
                searchSpaceTR[i] = wholeSearchSpaceTR[i];
                queryRegionBL[i] = wholeQueryRegionBL[i];
                queryRegionTR[i] = wholeQueryRegionTR[i];
            }
            state = 0;
            treeLevel = 1;
            minDerivedKey = 0;
            maxDerivedKey = (1 << DIMENSION) - 1;
            H = 0;
            nextMatch = 0;
            binarySearchIterationCount = 0;
        }

        public boolean areQueryRegionAndSearchSpaceEqual() {
            return searchSpaceBL[0] == queryRegionBL[0] && searchSpaceBL[1] == queryRegionBL[1]
                    && searchSpaceTR[0] == queryRegionTR[0] && searchSpaceTR[1] == queryRegionTR[1];
        }

        public void resetDerivedKeys() {
            minDerivedKey = 0;
            maxDerivedKey = (1 << DIMENSION) - 1;
        }

        public void updateDerivedKeysForUpperHalf() {
            minDerivedKey = (minDerivedKey + maxDerivedKey) / 2 + 1;
            /* maxDerivedKey doesn't change */
        }

        public void updateDerivedKeysForLowerHalf() {
            maxDerivedKey = (minDerivedKey + maxDerivedKey) / 2;
            /* minDerivedKey doesn't change */
        }

        public void updateNextMatch() {
            nextMatch += ((long) minDerivedKey) << (DIMENSION * (MAX_TREE_LEVEL - treeLevel));
        }

        public void updateState() {
            state = states[state].nextState[minDerivedKey];
        }

        public void reduceSearchSpace() {
            switch (cutDimension) {
                case 0: /* cut along the y axis */
                    if (takeHigherCoordinates) {
                        //push searchSpaceBL.y up (y is in index 1)
                        searchSpaceBL[1] += (searchSpaceTR[1] - searchSpaceBL[1]) / 2;
                        //adjust query region
                        if (queryRegionBL[1] < searchSpaceBL[1]) {
                            queryRegionBL[1] = searchSpaceBL[1];
                        }
                    } else {
                        //push searchSpaceTR.y down (y is in index 1)
                        searchSpaceTR[1] -= (searchSpaceTR[1] - searchSpaceBL[1]) / 2;
                        //adjust query region
                        if (queryRegionTR[1] > searchSpaceTR[1]) {
                            queryRegionTR[1] = searchSpaceTR[1];
                        }
                    }
                    break;

                case 1: /* cut along the x axis */
                    if (takeHigherCoordinates) {
                        //push searchSpaceBL.x right (x is in index 0)
                        searchSpaceBL[0] += (searchSpaceTR[0] - searchSpaceBL[0]) / 2;
                        //adjust query region
                        if (queryRegionBL[0] < searchSpaceBL[0]) {
                            queryRegionBL[0] = searchSpaceBL[0];
                        }
                    } else {
                        //push searchSpaceTR.x left (x is in index 0)
                        searchSpaceTR[0] -= (searchSpaceTR[0] - searchSpaceBL[0]) / 2;
                        //adjust query region
                        if (queryRegionTR[0] > searchSpaceTR[0]) {
                            queryRegionTR[0] = searchSpaceTR[0];
                        }
                    }
                    break;
            }
        }

        public void computeQLower() {
            qLower = HilbertValueBTreeRangeSearchCursorHelper.concatenateKthMSBs(queryRegionBL[0], queryRegionBL[1],
                    treeLevel, MAX_TREE_LEVEL);
        }

        public void computeQUpper() {
            /* corner case handling - deal with queryRegionTR which is on searchSpaceTR */
            long x = queryRegionTR[0] == searchSpaceTR[0] ? queryRegionTR[0] - 1 : queryRegionTR[0];
            long y = queryRegionTR[1] == searchSpaceTR[1] ? queryRegionTR[1] - 1 : queryRegionTR[1];
            qUpper = HilbertValueBTreeRangeSearchCursorHelper.concatenateKthMSBs(x, y, treeLevel, MAX_TREE_LEVEL);
        }

        public void computeH() {
            H = HilbertValueBTreeRangeSearchCursorHelper.get2BitsFromKthMSB(pageKey, treeLevel * 2 - 1,
                    MAX_TREE_LEVEL * 2);
        }
    }

    private class HilbertState {
        public final int[] nextState;
        public final int[] sn;
        public final int[] coord;

        public HilbertState(int[] nextState, int[] sn, int[] coord) {
            this.nextState = nextState; //using sn as an array index (not coord)
            this.sn = sn; //using coord as an array index
            this.coord = coord; //using sn as an array index
        }
    }
}
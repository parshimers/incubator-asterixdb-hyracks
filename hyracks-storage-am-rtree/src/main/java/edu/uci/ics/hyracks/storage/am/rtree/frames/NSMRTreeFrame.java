package edu.uci.ics.hyracks.storage.am.rtree.frames;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.EntriesOrder;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class NSMRTreeFrame extends TreeIndexNSMFrame implements IRTreeFrame {

    protected static final int pageNsnOff = smFlagOff + 1;
    private static final int rightPageOff = pageNsnOff + 4;

    private ITreeIndexTupleReference[] tuples;

    private ITreeIndexTupleReference cmpFrameTuple;

    private static final double reinsertFactor = 0.3;
    private static final double splitFactor = 0.4;
    private static final int nearMinimumOverlapFactor = 32;

    public NSMRTreeFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new UnorderedSlotManager());
        // TODO: change this to number of dim * 2 (at least it must be 4)
        this.tuples = new ITreeIndexTupleReference[4];
        for (int i = 0; i < 4; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();
    }
    
    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(pageNsnOff, -1);
        buf.putInt(rightPageOff, -1);
    }

    public void setTupleCount(int tupleCount) {
        buf.putInt(tupleCountOff, tupleCount);
    }

    @Override
    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, rightPageOff + 4);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (rightPageOff + 4));
    }

    @Override
    public int getRightPage() {
        return buf.getInt(rightPageOff);
    }

    @Override
    public void setRightPage(int rightPage) {
        buf.putInt(rightPageOff, rightPage);
    }

    public ITreeIndexTupleReference[] getTuples() {
        return tuples;
    }

    public void setTuples(ITreeIndexTupleReference[] tuples) {
        this.tuples = tuples;
    }

    // for debugging
    public ArrayList<Integer> getChildren(MultiComparator cmp) {
        ArrayList<Integer> ret = new ArrayList<Integer>();
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleCount = buf.getInt(tupleCountOff);
        for (int i = 0; i < tupleCount; i++) {
            int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(i));
            frameTuple.resetByTupleOffset(buf, tupleOff);

            int intVal = IntegerSerializerDeserializer.getInt(
                    buf.array(),
                    frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                            + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
            ret.add(intVal);
        }
        return ret;
    }

    // @Override
    // public int split(ITreeIndexFrame rightFrame, ITupleReference tuple,
    // MultiComparator cmp, ISplitKey splitKey)
    // throws Exception {
    //
    // RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
    // RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame =
    // ((RTreeTypeAwareTupleWriter) tupleWriter);
    // RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame =
    // ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());
    // frameTuple.setFieldCount(cmp.getFieldCount());
    // rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
    //
    // ByteBuffer right = rightFrame.getBuffer();
    // int tupleCount = getTupleCount();
    //
    // int tuplesToLeft;
    // int mid = tupleCount / 2;
    // ITreeIndexFrame targetFrame = null;
    // int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(mid));
    // frameTuple.resetByTupleOffset(buf, tupleOff);
    // if (cmp.compare(tuple, frameTuple) >= 0) {
    // tuplesToLeft = mid + (tupleCount % 2);
    // targetFrame = rightFrame;
    // } else {
    // tuplesToLeft = mid;
    // targetFrame = this;
    // }
    // int tuplesToRight = tupleCount - tuplesToLeft;
    //
    // // copy entire page
    // System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
    //
    // // on right page we need to copy rightmost slots to left
    // int src = rightFrame.getSlotManager().getSlotEndOff();
    // int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft
    // * rightFrame.getSlotManager().getSlotSize();
    // int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
    // System.arraycopy(right.array(), src, right.array(), dest, length);
    // right.putInt(tupleCountOff, tuplesToRight);
    //
    // // on left page only change the tupleCount indicator
    // buf.putInt(tupleCountOff, tuplesToLeft);
    //
    // // compact both pages
    // rightFrame.compact(cmp);
    // compact(cmp);
    //
    // // insert last key
    // targetFrame.insert(tuple, cmp);
    //
    // // set split key to be highest value in left page
    // // TODO: find a better way to find the key size
    // tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
    // frameTuple.resetByTupleOffset(buf, tupleOff);
    //
    // int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0,
    // cmp.getKeyFieldCount());
    // splitKey.initData(splitKeySize);
    // this.adjustNodeMBR(tuples, cmp);
    // rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0,
    // rTreeSplitKey.getLeftPageBuffer(), 0);
    // rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(),
    // 0);
    //
    // ((IRTreeFrame) rightFrame).adjustNodeMBR(((NSMRTreeFrame)
    // rightFrame).getTuples(), cmp);
    // rTreeTupleWriterRightFrame.writeTupleFields(((NSMRTreeFrame)
    // rightFrame).getTuples(), 0,
    // rTreeSplitKey.getRightPageBuffer(), 0);
    // rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(),
    // 0);
    //
    // return 0;
    // }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey,
            TupleEntryArrayList entries1, TupleEntryArrayList entries2, Rectangle[] rec) throws Exception {

        // RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        // RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame =
        // ((RTreeTypeAwareTupleWriter) tupleWriter);
        // RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame =
        // ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());
        // frameTuple.setFieldCount(cmp.getFieldCount());
        // rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
        //
        // ByteBuffer right = rightFrame.getBuffer();
        // int tupleCount = getTupleCount();
        //
        // int tuplesToLeft;
        // int mid = tupleCount / 2;
        // ITreeIndexFrame targetFrame = null;
        // int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(mid));
        // frameTuple.resetByTupleOffset(buf, tupleOff);
        // if (cmp.compare(tuple, frameTuple) >= 0) {
        // tuplesToLeft = mid + (tupleCount % 2);
        // targetFrame = rightFrame;
        // } else {
        // tuplesToLeft = mid;
        // targetFrame = this;
        // }
        // int tuplesToRight = tupleCount - tuplesToLeft;
        //
        // // copy entire page
        // System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
        //
        // // on right page we need to copy rightmost slots to left
        // int src = rightFrame.getSlotManager().getSlotEndOff();
        // int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft
        // * rightFrame.getSlotManager().getSlotSize();
        // int length = rightFrame.getSlotManager().getSlotSize() *
        // tuplesToRight;
        // System.arraycopy(right.array(), src, right.array(), dest, length);
        // right.putInt(tupleCountOff, tuplesToRight);
        //
        // // on left page only change the tupleCount indicator
        // buf.putInt(tupleCountOff, tuplesToLeft);
        //
        // // compact both pages
        // rightFrame.compact(cmp);
        // compact(cmp);
        //
        // // insert last key
        // targetFrame.insert(tuple, cmp);
        //
        // // set split key to be highest value in left page
        // // TODO: find a better way to find the key size
        // tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        // frameTuple.resetByTupleOffset(buf, tupleOff);
        //
        // int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0,
        // cmp.getKeyFieldCount());
        // splitKey.initData(splitKeySize);
        // this.adjustNodeMBR(tuples, cmp);
        // rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0,
        // rTreeSplitKey.getLeftPageBuffer(), 0);
        // rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(),
        // 0);
        //
        // ((IRTreeFrame) rightFrame).adjustNodeMBR(((NSMRTreeFrame)
        // rightFrame).getTuples(), cmp);
        // rTreeTupleWriterRightFrame.writeTupleFields(((NSMRTreeFrame)
        // rightFrame).getTuples(), 0,
        // rTreeSplitKey.getRightPageBuffer(), 0);
        // rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(),
        // 0);
        //
        // return 0;

        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());
        rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
        frameTuple.setFieldCount(cmp.getFieldCount());
        cmpFrameTuple.setFieldCount(cmp.getFieldCount());

        // calculations are based on the R*-tree paper
        int m = (int) Math.floor((getTupleCount() + 1) * splitFactor);
        int splitDistribution = getTupleCount() - (2 * m) + 2;

        // to calculate the minimum margin in order to pick the split axis
        double minMargin = Double.MAX_VALUE;
        int splitAxis = 0, sortOrder = 0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            for (int k = 0; k < getTupleCount(); ++k) {

                frameTuple.resetByTupleIndex(this, k);

                double LowerKey = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(i),
                        frameTuple.getFieldStart(i));
                double UpperKey = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j));

                entries1.add(k, LowerKey);
                entries2.add(k, UpperKey);
            }
            double LowerKey = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
            double UpperKey = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j));

            entries1.add(-1, LowerKey);
            entries2.add(-1, UpperKey);

            entries1.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);
            entries2.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);

            double lowerMargin = 0.0, upperMargin = 0.0;
            // generate distribution
            for (int k = 1; k <= splitDistribution; ++k) {
                int d = m - 1 + k;

                generateDist(tuple, entries1, rec[0], 0, d);
                generateDist(tuple, entries2, rec[1], 0, d);
                generateDist(tuple, entries1, rec[2], d, getTupleCount() + 1);
                generateDist(tuple, entries2, rec[3], d, getTupleCount() + 1);

                // calculate the margin of the distributions
                lowerMargin += rec[0].margin() + rec[2].margin();
                upperMargin += rec[1].margin() + rec[3].margin();
            }
            double margin = Math.min(lowerMargin, upperMargin);

            // store minimum margin as split axis
            if (margin < minMargin) {
                minMargin = margin;
                splitAxis = i;
                sortOrder = (lowerMargin < upperMargin) ? 0 : 2;
            }

            entries1.clear();
            entries2.clear();
        }

        for (int i = 0; i < getTupleCount(); ++i) {
            frameTuple.resetByTupleIndex(this, i);
            double key = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(splitAxis + sortOrder),
                    frameTuple.getFieldStart(splitAxis + sortOrder));
            entries1.add(i, key);
        }
        double key = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(splitAxis + sortOrder),
                tuple.getFieldStart(splitAxis + sortOrder));
        entries1.add(-1, key);
        entries1.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);

        double minArea = Double.MAX_VALUE;
        double minOverlap = Double.MAX_VALUE;
        int splitPoint = 0;
        for (int i = 1; i <= splitDistribution; ++i) {
            int d = m - 1 + i;

            generateDist(tuple, entries1, rec[0], 0, d);
            generateDist(tuple, entries1, rec[2], d, getTupleCount() + 1);

            double overlap = rec[0].overlappedArea(rec[2]);
            if (overlap < minOverlap) {
                splitPoint = d;
                minOverlap = overlap;
                minArea = rec[0].area() + rec[2].area();
            } else if (overlap == minOverlap) {
                double area = rec[0].area() + rec[2].area();
                if (area < minArea) {
                    splitPoint = d;
                    minArea = area;
                }
            }
        }
        int startIndex, endIndex;
        if (splitPoint < (getTupleCount() + 1) / 2) {
            startIndex = 0;
            endIndex = splitPoint;
        } else {
            startIndex = splitPoint;
            endIndex = (getTupleCount() + 1);
        }
        boolean tupleInserted = false;
        int totalBytes = 0, numOfDeletedTuples = 0;
        for (int i = startIndex; i < endIndex; i++) { // TODO: is there a better
                                                      // way
            // to split the entries?
            if (entries1.get(i).getTupleIndex() != -1) {
                frameTuple.resetByTupleIndex(this, entries1.get(i).getTupleIndex());
                rightFrame.insert(frameTuple, cmp);
                ((UnorderedSlotManager) slotManager).modifySlot(
                        slotManager.getSlotOff(entries1.get(i).getTupleIndex()), -1);
                totalBytes += tupleWriter.bytesRequired(frameTuple);
                numOfDeletedTuples++;
            } else {
                rightFrame.insert(tuple, cmp);
                tupleInserted = true;
            }
        }

        ((UnorderedSlotManager) slotManager).deleteEmptySlots();

        // maintain space information
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + totalBytes
                + (slotManager.getSlotSize() * numOfDeletedTuples));

        if (!tupleInserted) {
            insert(tuple, cmp);
        }

        // compact both pages
        rightFrame.compact(cmp);
        compact(cmp);

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());

        splitKey.initData(splitKeySize);
        this.adjustNodeMBR(tuples, cmp);
        rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0, rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(), 0);

        ((IRTreeFrame) rightFrame).adjustNodeMBR(((NSMRTreeFrame) rightFrame).getTuples(), cmp);
        rTreeTupleWriterRightFrame.writeTupleFields(((NSMRTreeFrame) rightFrame).getTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(), 0);

        entries1.clear();
        return 0;
    }

    public void generateDist(ITupleReference tuple, TupleEntryArrayList entries, Rectangle rec, int start, int end) {
        int j = 0;
        while (entries.get(j).getTupleIndex() == -1) {
            j++;
        }
        frameTuple.resetByTupleIndex(this, entries.get(j).getTupleIndex());
        rec.set(frameTuple);
        for (int i = start; i < end; ++i) {
            if (i != j) {
                if (entries.get(i).getTupleIndex() != -1) {
                    frameTuple.resetByTupleIndex(this, entries.get(i).getTupleIndex());
                    rec.enlarge(frameTuple);
                } else {
                    rec.enlarge(tuple);
                }
            }
        }
    }

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        try {
            insert(tuple, cmp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getChildPageId(ITupleReference tuple, TupleEntryArrayList entries, ITreeIndexTupleReference[] nodesMBRs,
            MultiComparator cmp) {

        cmpFrameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.setFieldCount(cmp.getFieldCount());

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // the children pointers in the node point to leaves
        if (getLevel() == 1) {
            // find least overlap enlargement, use minimum enlarged area to
            // break tie, if tie still exists use minimum area to break it
            for (int i = 0; i < getTupleCount(); ++i) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                entries.add(i, enlargedArea);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                }
            }
            if (minEnlargedArea < entries.getDoubleEpsilon() || minEnlargedArea > entries.getDoubleEpsilon()) {
                minEnlargedArea = Double.MAX_VALUE;
                int k;
                if (getTupleCount() > nearMinimumOverlapFactor) {
                    // sort the entries based on their area enlargement needed
                    // to include the object
                    entries.sort(EntriesOrder.ASCENDING, getTupleCount());
                    k = nearMinimumOverlapFactor;
                } else {
                    k = getTupleCount();
                }

                double minOverlap = Double.MAX_VALUE;
                int id = 0;
                for (int i = 0; i < k; ++i) {
                    double difference = 0.0;
                    for (int j = 0; j < getTupleCount(); ++j) {
                        frameTuple.resetByTupleIndex(this, j);
                        cmpFrameTuple.resetByTupleIndex(this, entries.get(i).getTupleIndex());

                        int c = cmp.getIntCmp().compare(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                                frameTuple.getFieldStart(cmp.getKeyFieldCount()),
                                frameTuple.getFieldLength(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldData(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldStart(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldLength(cmp.getKeyFieldCount()));
                        if (c != 0) {
                            double intersection = overlappedArea(frameTuple, tuple, cmpFrameTuple, cmp);
                            if (intersection != 0.0) {
                                difference += intersection - overlappedArea(frameTuple, null, cmpFrameTuple, cmp);
                            }
                        } else {
                            id = j;
                        }
                    }

                    double enlargedArea = enlargedArea(cmpFrameTuple, tuple, cmp);
                    if (difference < minOverlap) {
                        minOverlap = difference;
                        minEnlargedArea = enlargedArea;
                        bestChild = id;
                    } else if (difference == minOverlap) {
                        if (enlargedArea < minEnlargedArea) {
                            minEnlargedArea = enlargedArea;
                            bestChild = id;
                        } else if (enlargedArea == minEnlargedArea) {
                            double area = area(cmpFrameTuple, cmp);
                            frameTuple.resetByTupleIndex(this, bestChild);
                            double minArea = area(frameTuple, cmp);
                            if (area < minArea) {
                                bestChild = id;
                            }
                        }
                    }
                }
            }
        } else { // find minimum enlarged area, use minimum area to break tie
            for (int i = 0; i < getTupleCount(); i++) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                } else if (enlargedArea == minEnlargedArea) {
                    double area = area(frameTuple, cmp);
                    frameTuple.resetByTupleIndex(this, bestChild);
                    double minArea = area(frameTuple, cmp);
                    if (area < minArea) {
                        bestChild = i;
                    }
                }
            }
        }
        frameTuple.resetByTupleIndex(this, bestChild);
        if (minEnlargedArea > 0.0) {
            enlarge(frameTuple, tuple, cmp);
        }
        nodesMBRs[(int) getLevel() - 1].resetByTupleIndex(this, bestChild);

        entries.clear();

        // return the page id of the bestChild tuple
        return buf.getInt(frameTuple.getFieldStart(cmp.getKeyFieldCount()));
    }

    @Override
    public void reinsert(ITupleReference tuple, ITreeIndexTupleReference nodeMBR, TupleEntryArrayList entries,
            ISplitKey splitKey, MultiComparator cmp) throws Exception {

        nodeMBR.setFieldCount(cmp.getFieldCount());
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < getTupleCount() + 1; ++i) {
            if (i < getTupleCount()) {
                frameTuple.resetByTupleIndex(this, i);
            }
            double centerDistance = 0.0;
            for (int j = 0; j < maxFieldPos; j++) {
                int k = maxFieldPos + j;

                // TODO: an optimization can be done here, compute nodeCenter
                // only once and reuse it
                double nodeCenter = (DoubleSerializerDeserializer.getDouble(nodeMBR.getFieldData(j),
                        nodeMBR.getFieldStart(j)) + DoubleSerializerDeserializer.getDouble(nodeMBR.getFieldData(k),
                        nodeMBR.getFieldStart(k))) / 2.0;

                double childCenter;
                if (i < getTupleCount()) {
                    childCenter = (DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(j),
                            frameTuple.getFieldStart(j)) + DoubleSerializerDeserializer.getDouble(
                            frameTuple.getFieldData(k), frameTuple.getFieldStart(k))) / 2.0;
                } else { // special case to deal with the new tuple to be
                         // inserted
                    childCenter = (DoubleSerializerDeserializer
                            .getDouble(tuple.getFieldData(j), tuple.getFieldStart(j)) + DoubleSerializerDeserializer
                            .getDouble(tuple.getFieldData(k), tuple.getFieldStart(k))) / 2.0;
                }

                double d = childCenter - nodeCenter;
                centerDistance += d * d;
            }
            if (i < getTupleCount()) {
                entries.add(i, centerDistance);
            } else { // special case to deal with the new tuple to be inserted
                entries.add(-1, centerDistance);
            }

        }
        entries.sort(EntriesOrder.DESCENDING, getTupleCount() + 1);

        int j = (int) Math.floor((getTupleCount() + 1) * reinsertFactor);
        for (int i = 0; i < j; i++) {
            if (entries.get(i).getTupleIndex() != -1) {
                frameTuple.resetByTupleIndex(this, i);
                delete(frameTuple, cmp, false);
            } else {
                delete(tuple, cmp, false);
            }

        }

        // rebuild the node's MBR
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        this.adjustNodeMBR(tuples, cmp);

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        rTreeSplitKey.initData(splitKeySize);

        rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0, rTreeSplitKey.getLeftPageBuffer(), 0);

        entries.clear();
    }

    public double area(ITupleReference tuple, MultiComparator cmp) {
        double area = 1.0;
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            area *= DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }

    public double overlappedArea(ITupleReference tuple1, ITupleReference tupleToBeInserted, ITupleReference tuple2,
            MultiComparator cmp) {
        double area = 1.0;
        double f1, f2;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh1, pLow1;
            if (tupleToBeInserted != null) {
                int c = cmp.getComparators()[i].compare(tuple1.getFieldData(i), tuple1.getFieldStart(i),
                        tuple1.getFieldLength(i), tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i), tupleToBeInserted.getFieldLength(i));
                if (c < 0) {
                    pLow1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                } else {
                    pLow1 = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(i),
                            tupleToBeInserted.getFieldStart(i));
                }

                c = cmp.getComparators()[j].compare(tuple1.getFieldData(j), tuple1.getFieldStart(j),
                        tuple1.getFieldLength(j), tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j), tupleToBeInserted.getFieldLength(j));
                if (c > 0) {
                    pHigh1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(j), tuple1.getFieldStart(j));
                } else {
                    pHigh1 = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(j),
                            tupleToBeInserted.getFieldStart(j));
                }
            } else {
                pLow1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                pHigh1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(j), tuple1.getFieldStart(j));
            }

            double pLow2 = DoubleSerializerDeserializer.getDouble(tuple2.getFieldData(i), tuple2.getFieldStart(i));
            double pHigh2 = DoubleSerializerDeserializer.getDouble(tuple2.getFieldData(j), tuple2.getFieldStart(j));

            if (pLow1 > pHigh2 || pHigh1 < pLow2) {
                return 0.0;
            }

            f1 = Math.max(pLow1, pLow2);
            f2 = Math.min(pHigh1, pHigh2);
            area *= f2 - f1;
        }
        return area;
    }

    public double enlargedArea(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
        double areaBeforeEnlarge = area(tuple, cmp);
        double areaAfterEnlarge = 1.0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh, pLow;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c < 0) {
                pLow = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
            } else {
                pLow = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i));
            }

            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c > 0) {
                pHigh = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j));
            } else {
                pHigh = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j));
            }
            areaAfterEnlarge *= pHigh - pLow;
        }
        return areaAfterEnlarge - areaBeforeEnlarge;
    }

    public void enlarge(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c > 0) {
                System.arraycopy(tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                        tuple.getFieldData(i), tuple.getFieldStart(i), tupleToBeInserted.getFieldLength(i));
            }
            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c < 0) {
                System.arraycopy(tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                        tuple.getFieldData(j), tuple.getFieldStart(j), tupleToBeInserted.getFieldLength(j));
            }
        }
    }

    @Override
    public void adjustKey(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
        if (tupleIndex != -1) {
            tupleWriter.writeTuple(tuple, buf, getTupleOffset(tupleIndex));
        }
    }

    @Override
    public void adjustNodeMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp) {
        for (int i = 0; i < tuples.length; i++) {
            tuples[i].setFieldCount(cmp.getKeyFieldCount());
            tuples[i].resetByTupleIndex(this, 0);
        }

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 1; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < maxFieldPos; j++) {
                int k = maxFieldPos + j;
                int c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                        frameTuple.getFieldLength(j), tuples[j].getFieldData(j), tuples[j].getFieldStart(j),
                        tuples[j].getFieldLength(j));
                if (c < 0) {
                    tuples[j].resetByTupleIndex(this, i);
                }
                c = cmp.getComparators()[k].compare(frameTuple.getFieldData(k), frameTuple.getFieldStart(k),
                        frameTuple.getFieldLength(k), tuples[k].getFieldData(k), tuples[k].getFieldStart(k),
                        tuples[k].getFieldLength(k));
                if (c > 0) {
                    tuples[k].resetByTupleIndex(this, i);
                }
            }
        }
    }

    @Override
    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return -1;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return -1;
            }
        }
        return buf.getInt(frameTuple.getFieldStart(cmp.getKeyFieldCount()));
    }

    @Override
    public Rectangle intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return null;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return null;
            }
        }
        Rectangle rec = new Rectangle(maxFieldPos);
        rec.set(frameTuple);
        return rec;
    }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
}

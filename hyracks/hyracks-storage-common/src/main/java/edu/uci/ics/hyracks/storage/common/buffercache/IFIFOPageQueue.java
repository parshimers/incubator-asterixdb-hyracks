package edu.uci.ics.hyracks.storage.common.buffercache;

public interface IFIFOPageQueue {
    public void put(ICachedPage page);
}

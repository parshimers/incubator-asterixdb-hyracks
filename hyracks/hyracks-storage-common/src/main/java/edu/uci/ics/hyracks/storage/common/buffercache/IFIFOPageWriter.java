package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;


public interface IFIFOPageWriter {
    public void write(ICachedPage page, IBufferCache bufferCache) throws HyracksDataException;
}

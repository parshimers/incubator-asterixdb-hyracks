package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class FIFOLocalWriter implements IFIFOPageWriter {
    private static FIFOLocalWriter instance;
    private static boolean DEBUG = true;

    public static FIFOLocalWriter instance() {
        if(instance == null) {
            instance = new FIFOLocalWriter();
        }
        return instance;
    }

    @Override
    public void write(ICachedPage page, IBufferCache ibufferCache) throws HyracksDataException {
        BufferCache bufferCache = (BufferCache)ibufferCache;
        CachedPage cPage = (CachedPage)page;
        BufferedFileHandle fInfo = bufferCache.getFileInfo(cPage);
        if (fInfo.fileHasBeenDeleted()) {
            return;
        }
        cPage.buffer.position(0);
        cPage.buffer.limit(bufferCache.getPageSize());
        bufferCache.ioManager.syncWrite(fInfo.getFileHandle(), (long) BufferedFileHandle.getPageId(cPage.dpid) * bufferCache.getPageSize(),
                cPage.buffer);
        bufferCache.returnPage(cPage);
        if(DEBUG) System.out.println("[FIFO] Return page");
    }
    
    @Override
    public void sync(int fileId, IBufferCache ibufferCache) throws HyracksDataException {
        BufferCache bufferCache = (BufferCache)ibufferCache;
        BufferedFileHandle fInfo = bufferCache.getFileInfo(fileId);
        bufferCache.ioManager.sync(fInfo.getFileHandle(), true);
    }
}

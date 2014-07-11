package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFilePath;

public interface IDFSBufferCache {

    public void createFile(IFilePath fileRef) throws HyracksDataException;

    public int createMemFile() throws HyracksDataException;

    public void openFile(int fileId) throws HyracksDataException;

    public void closeFile(int fileId) throws HyracksDataException;

    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException;

    public void deleteMemFile(int fileId) throws HyracksDataException;

    public ICachedPage tryPin(long dpid) throws HyracksDataException;

    public ICachedPage pinVirtual(long vpid) throws HyracksDataException;
    
    public ICachedPage unpinVirtual(long vpid, long dpid) throws HyracksDataException;

    public void unpin(ICachedPage page) throws HyracksDataException;

    public void force(int fileId, boolean metadata) throws HyracksDataException;

    public int getPageSize();

    public int getNumPages();

    public void close() throws HyracksDataException;

    public ICachedPage pin(long diskPageId, boolean b) throws HyracksDataException;
}

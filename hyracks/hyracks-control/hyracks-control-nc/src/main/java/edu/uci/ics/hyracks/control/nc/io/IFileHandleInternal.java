package edu.uci.ics.hyracks.control.nc.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public interface IFileHandleInternal extends IFileHandle {

    abstract void open(IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode) throws IOException;

    abstract void close() throws IOException;

    abstract FileReference getFileReference();

    abstract RandomAccessFile getRandomAccessFile();

    abstract void sync(boolean metadata) throws IOException;

    abstract long getSize() throws IOException;

    abstract int write(ByteBuffer data, long offset) throws IOException;

    abstract int read(ByteBuffer data, long offset) throws IOException;

}
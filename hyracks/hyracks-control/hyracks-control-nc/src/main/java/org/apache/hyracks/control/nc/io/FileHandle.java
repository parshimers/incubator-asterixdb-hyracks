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
package org.apache.hyracks.control.nc.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;

public class FileHandle implements IFileHandle, IFileHandleInternal {
    private final FileReference fileRef;

    private RandomAccessFile raf;

    private FileChannel channel;

    public FileHandle(FileReference fileRef) {
        this.fileRef = fileRef;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.control.nc.io.IFileHandleInternal#open(edu.uci.ics.hyracks.api.io.IIOManager.FileReadWriteMode, edu.uci.ics.hyracks.api.io.IIOManager.FileSyncMode)
     */
    @Override
    public void open(IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode) throws IOException {
        String mode;
        switch (rwMode) {
            case READ_ONLY:
                mode = "r";
                break;

            case READ_WRITE:
                fileRef.getFile().getAbsoluteFile().getParentFile().mkdirs();
                switch (syncMode) {
                    case METADATA_ASYNC_DATA_ASYNC:
                        mode = "rw";
                        break;

                    case METADATA_ASYNC_DATA_SYNC:
                        mode = "rwd";
                        break;

                    case METADATA_SYNC_DATA_SYNC:
                        mode = "rws";
                        break;

                    default:
                        throw new IllegalArgumentException();
                }
                break;

            default:
                throw new IllegalArgumentException();
        }
        raf = new RandomAccessFile(fileRef.getFile(), mode);
        channel = raf.getChannel();
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.control.nc.io.IFileHandleInternal#close()
     */
    @Override
    public void close() throws IOException {
        channel.close();
        raf.close();
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.control.nc.io.IFileHandleInternal#getFileReference()
     */
    @Override
    public FileReference getFileReference() {
        return fileRef;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.control.nc.io.IFileHandleInternal#getRandomAccessFile()
     */
    @Override
    public RandomAccessFile getRandomAccessFile() {
        return raf;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.control.nc.io.IFileHandleInternal#sync(boolean)
     */
    @Override
    public void sync(boolean metadata) throws IOException {
        channel.force(metadata);
    }

    @Override
    public long getSize() {
        return getFileReference().getFile().length();
    }

    @Override
    public int write(ByteBuffer data, long offset) throws IOException {
        //System.out.println("Local write: "+ offset);
        return channel.write(data, offset);
    }

    @Override
    public int append(ByteBuffer data) throws IOException {
        //System.out.println("Local write: "+ channel.position());
        return channel.write(data);
    }

    @Override
    public int read(ByteBuffer data, long offset) throws IOException {
        //System.out.println("Local read: "+ offset);
        return channel.read(data, offset);
    }

    @Override
    public InputStream getInputStream(){
        return Channels.newInputStream(raf.getChannel());
    }
}

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
package org.apache.hyracks.control.nc.io;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileReference.FileReferenceType;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOFuture;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;

public class IOManager implements IIOManager {

    public static final boolean DEBUG = true;
    private static final String WORKSPACE_FILE_SUFFIX = ".waf";
    private final List<IODeviceHandle> ioDevices;
    private Executor executor;
    private final List<IODeviceHandle> workAreaIODevices;
    private int workAreaDeviceIndex;
    private String hdpConfPath;
    
    private IIOSubSystem[] ioSubSystems = new IIOSubSystem[FileReferenceType.values().length];

    public IOManager(List<IODeviceHandle> devices, Executor executor) throws HyracksException {
        this(devices);
        this.executor = executor;
    }

    public IOManager(List<IODeviceHandle> devices) throws HyracksException {
        this.ioDevices = Collections.unmodifiableList(devices);
        workAreaIODevices = new ArrayList<IODeviceHandle>();
        for (IODeviceHandle d : ioDevices) {
            if (d.getWorkAreaPath() != null) {
                new File(d.getPath(), d.getWorkAreaPath()).mkdirs();
                workAreaIODevices.add(d);
            }
        }
        if (workAreaIODevices.isEmpty()) {
            throw new HyracksException("No devices with work areas found");
        }
        workAreaDeviceIndex = 0;
    }

    public IOManager(List<IODeviceHandle> devices, String hdpConfPath) throws HyracksException {
        this(devices);
        this.hdpConfPath = hdpConfPath;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public List<IODeviceHandle> getIODevices() {
        return ioDevices;
    }

    @Override
    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        IFileHandleInternal fHandle = fileRef.getType() == FileReferenceType.LOCAL ? new FileHandle(fileRef) : new HDFSFileHandle(fileRef);
        try {
            fHandle.open(rwMode, syncMode);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return fHandle;
    }

    @Override
    public int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        if(DEBUG){
//            System.out.println("Write: "+offset);
        }
        try {
            if (fHandle == null) {
                throw new IllegalStateException("Trying to write to a deleted file.");
            }
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((IFileHandleInternal) fHandle).write(data, offset);
                if (len < 0) {
                    throw new HyracksDataException("Error writing to file: "
                            + ((IFileHandleInternal) fHandle).getFileReference().toString());
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * Please do check the return value of this read!
     *
     * @param fHandle
     * @param data
     * @return The number of bytes read, possibly zero, or -1 if the given offset is greater than or equal to the file's current size
     * @throws HyracksDataException
     */
    @Override
    public int append(IFileHandle fHandle, ByteBuffer data) throws HyracksDataException{
        try {
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((IFileHandleInternal) fHandle).append(data);
                if (len < 0) {
                    throw new HyracksDataException("Error writing to file: "
                            + ((IFileHandleInternal) fHandle).getFileReference().toString());
                }
                remaining -= len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        try {
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((IFileHandleInternal) fHandle).read(data, offset);
                if (len < 0) {
                    return n == 0 ? -1 : n;
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public IIOFuture asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncWriteRequest req = new AsyncWriteRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public IIOFuture asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncReadRequest req = new AsyncReadRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public void close(IFileHandle fHandle) throws HyracksDataException {
        try {
            ((IFileHandleInternal) fHandle).close();
        } catch (IOException e) {
//            throw new HyracksDataException(e);
        }
    }

    public synchronized FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        IODeviceHandle dev = workAreaIODevices.get(workAreaDeviceIndex);
        workAreaDeviceIndex = (workAreaDeviceIndex + 1) % workAreaIODevices.size();
        String waPath = dev.getWorkAreaPath();
        File waf;
        try {
            waf = File.createTempFile(prefix, WORKSPACE_FILE_SUFFIX, new File(dev.getPath(), waPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return dev.createFileReference(waPath + File.separator + waf.getName());
    }

    private abstract class AsyncRequest implements IIOFuture, Runnable {
        protected final FileHandle fHandle;
        protected final long offset;
        protected final ByteBuffer data;
        private boolean complete;
        private HyracksDataException exception;
        private int result;

        private AsyncRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            this.fHandle = fHandle;
            this.offset = offset;
            this.data = data;
            complete = false;
            exception = null;
        }

        @Override
        public void run() {
            HyracksDataException hde = null;
            int res = -1;
            try {
                res = performOperation();
            } catch (HyracksDataException e) {
                hde = e;
            }
            synchronized (this) {
                exception = hde;
                result = res;
                complete = true;
                notifyAll();
            }
        }

        protected abstract int performOperation() throws HyracksDataException;

        @Override
        public synchronized int synchronize() throws HyracksDataException, InterruptedException {
            while (!complete) {
                wait();
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        }

        @Override
        public synchronized boolean isComplete() {
            return complete;
        }
    }

    private class AsyncReadRequest extends AsyncRequest {
        private AsyncReadRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncRead(fHandle, offset, data);
        }
    }

    private class AsyncWriteRequest extends AsyncRequest {
        private AsyncWriteRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncWrite(fHandle, offset, data);
        }
    }

    @Override
    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException {
        try {
            ((IFileHandleInternal) fileHandle).sync(metadata);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public long getSize(IFileHandle fileHandle) throws HyracksDataException {
        try {
            return ((IFileHandleInternal) fileHandle).getSize();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
    @Override
    public boolean exists(FileReference fileReference) {
        try {
            return getIOSubSystem(fileReference).exists(fileReference);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean mkdirs(FileReference fileReference) {
        try {
            return getIOSubSystem(fileReference).mkdirs(fileReference);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean isDirectory(FileReference fileReference) {
        try {
            return getIOSubSystem(fileReference).isDirectory(fileReference);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean delete(FileReference fileReference) {
        try {
            return getIOSubSystem(fileReference).delete(fileReference, false);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean delete(FileReference fileReference, boolean recursive) {
        try {
            return getIOSubSystem(fileReference).delete(fileReference, true);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean deleteOnExit(FileReference fileReference){
        try{
            return getIOSubSystem(fileReference).deleteOnExit(fileReference);
        }
        catch(IllegalArgumentException | IOException e){
            throw new IllegalStateException(e);
        }
    }

    @Override
    public FileReference getParent(FileReference child){
        try{
            return getIOSubSystem(child).getParent(child);
        }
        catch(IllegalArgumentException | IOException e){
            throw new IllegalStateException(e);
        }
    }

    
    IIOSubSystem getIOSubSystem(FileReference fileReference) {
        IIOSubSystem ioSubSystem = ioSubSystems[fileReference.getType().ordinal()];
        if(ioSubSystem == null) {
            switch(fileReference.getType()) {
                case DISTRIBUTED_IF_AVAIL:
                    ioSubSystem = new IOHDFSSubSystem(hdpConfPath);
                    break;
                case LOCAL:
                    ioSubSystem = new IOLocalSubSystem();
                    break;
                default:
                    throw new IllegalStateException();
            }
            ioSubSystems[fileReference.getType().ordinal()] = ioSubSystem;
        }
        return ioSubSystem;
    }

    @Override
    public String[] listFiles(FileReference fileReference, FilenameFilter filter) throws HyracksDataException {
        try {
            return getIOSubSystem(fileReference).listFiles(fileReference, filter);
        } catch (IllegalArgumentException | IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public InputStream getInputStream(IFileHandle fileHandle) {
        try {
            return ((IFileHandleInternal)fileHandle).getInputStream();
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteWorkspaceFiles() {
        for (IODeviceHandle ioDevice : workAreaIODevices) {
            File workspaceFolder = new File(ioDevice.getPath(), ioDevice.getWorkAreaPath());
            if (workspaceFolder.exists() && workspaceFolder.isDirectory()) {
                File[] workspaceFiles = workspaceFolder.listFiles(WORKSPACE_FILES_FILTER);
                for (File workspaceFile : workspaceFiles) {
                    workspaceFile.delete();
                }
            }
        }
    }

    private static final FilenameFilter WORKSPACE_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return name.endsWith(WORKSPACE_FILE_SUFFIX);
        }
    };

    @Override
    public FileReference getAbsoluteFileRef(int ioDeviceId, String relativePath) {
        IODeviceHandle devHandle = ioDevices.get(ioDeviceId);
        return new FileReference(relativePath,FileReferenceType.DISTRIBUTED_IF_AVAIL);
    }
}

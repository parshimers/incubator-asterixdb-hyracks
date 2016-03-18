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
package org.apache.hyracks.api.io;

import java.io.File;
import java.io.Serializable;
import java.net.URI;

public final class FileReference implements Serializable {
    public enum FileReferenceType {LOCAL, DISTRIBUTED_IF_AVAIL};
    
    private static final long serialVersionUID = 1L;

    private final String path;
    public final FileReferenceType type;

    private File file;

    public FileReference(String path) {
        this.path = path;
        this.type = FileReferenceType.LOCAL;
    }
    
    public FileReference(String path, FileReferenceType type) {
        this.path = path;
        this.type = type;
    }
    
    public String getPath() {
        return path;
    }

    public String getName() {
        return path.split(File.separator)[path.split(File.separator).length-1];
    }
    
    public FileReferenceType getType() {
        return type;
    }

    @Override
    public String toString() {
        return path.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FileReference)) {
            return false;
        }
        return path.equals(((FileReference) o).path);
    }

    public String getAbsolutePath(){
        return path;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Deprecated
    public File getFile() {
        if(type != FileReferenceType.LOCAL) throw new IllegalStateException("Cannot use getFile for non-local files");
        if(file == null) file = new File(path);
        return file;
    }
}

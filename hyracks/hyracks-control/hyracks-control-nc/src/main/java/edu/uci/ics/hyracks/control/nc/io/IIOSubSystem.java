package edu.uci.ics.hyracks.control.nc.io;

import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import edu.uci.ics.hyracks.api.io.FileReference;

public interface IIOSubSystem {
    public boolean exists(FileReference fileRef) throws IllegalArgumentException, IOException;
    public boolean mkdirs(FileReference fileRef) throws IllegalArgumentException, IOException;
    public boolean delete(FileReference fileRef, boolean recursive) throws IllegalArgumentException, IOException;
    public boolean deleteOnExit(FileReference fileRef) throws IllegalArgumentException, IOException;
    public boolean isDirectory(FileReference fileRef) throws IllegalArgumentException, IOException;
    public String[] listFiles(FileReference fileRef, FilenameFilter filter) throws FileNotFoundException, IllegalArgumentException, IOException;
}

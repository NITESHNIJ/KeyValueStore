package org.niteshnij.core;

/**
 * Immutable pointer to where a key's latest value lives on disk.
 * Stored in the in-memory index; carries file name + byte offset.
 */
public class IndexLocation {
    private final String fileName;
    private final long offset;

    public IndexLocation(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }
}

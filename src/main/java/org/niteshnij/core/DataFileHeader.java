package org.niteshnij.core;

import org.niteshnij.io.DataFile;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Binary header written at byte 0 of every .db file.
 *
 * Layout (21 bytes total):
 *   [0-3]   Magic number  (0x1234ABCD) — sanity check on open
 *   [4]     File state    (0=active, 1=soft-deleted)
 *   [5-20]  MD5 checksum  of all bytes after the header
 *
 * Not thread-safe — callers must hold an exclusive lock on the file
 * before invoking any method here.
 */
public abstract class DataFileHeader {

    protected static final int HEADER_SIZE = 21;

    private byte fileState = 0;
    private byte[] checksum = new byte[16];

    public void writeHeader(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(0x1234ABCD);
        this.fileState = 0;
        this.checksum = calculateChecksum(file);
        buffer.put((byte) 0);
        buffer.put(this.checksum);
        file.seek(0);
        file.write(buffer.array());
    }

    public byte getFileState() {
        return fileState;
    }

    public void updateFileState(RandomAccessFile file, byte state) throws IOException {
        this.fileState = state;
        file.seek(4);
        file.writeByte(state);
    }

    public void readHeader(RandomAccessFile file) throws IOException {
        file.seek(0);
        byte[] headerBytes = new byte[HEADER_SIZE];
        file.readFully(headerBytes);
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        buffer.getInt(); // magic number — validated on open
        fileState = buffer.get();
        buffer.get(checksum);
    }

    private byte[] calculateChecksum(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        file.seek(HEADER_SIZE);
        byte[] buf = new byte[1024];
        int n;
        while ((n = file.read(buf)) != -1) {
            md.update(buf, 0, n);
        }
        return md.digest();
    }
}

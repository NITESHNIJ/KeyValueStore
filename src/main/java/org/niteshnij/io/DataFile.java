package org.niteshnij.io;

import org.niteshnij.core.DataFileHeader;
import org.niteshnij.core.IndexLocation;
import org.niteshnij.listener.DataFileSizeListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * A single append-only segment file on disk.
 *
 * Each entry is encoded as:
 *   [4 bytes] key length
 *   [4 bytes] value length
 *   [N bytes] key bytes (UTF-8)
 *   [M bytes] value bytes (UTF-8)
 *
 * The file begins with a DataFileHeader (21 bytes) that holds a magic
 * number, file state, and MD5 checksum of all data bytes.
 *
 * All public methods are synchronized — DataFile instances are shared
 * across the read thread pool and the single write thread.
 */
public class DataFile extends DataFileHeader {

    // Rotate to a new file once this size is reached (1 MB).
    public static final int MAX_FILE_SIZE = 1_000_000;

    public static class Pair {
        public String key;
        public Long offset;

        Pair(String key, Long offset) {
            this.key = key;
            this.offset = offset;
        }
    }

    public static class Entry {
        public String key;
        public String value;
        public long offset;

        Entry(String key, String value, long offset) {
            this.key = key;
            this.value = value;
            this.offset = offset;
        }
    }

    private final File file;
    private RandomAccessFile storeFile;
    private final List<DataFileSizeListener> sizeListeners = new ArrayList<>();

    public DataFile(String fileName) throws IOException, NoSuchAlgorithmException {
        this.file = new File(fileName);
        this.storeFile = new RandomAccessFile(this.file, "rw");
        this.writeHeader(this.storeFile);
    }

    public DataFile(File file) throws IOException, NoSuchAlgorithmException {
        this.file = file;
        this.storeFile = new RandomAccessFile(this.file, "rw");
        this.writeHeader(this.storeFile);
    }

    public void addDataFileSizeListener(DataFileSizeListener listener) {
        sizeListeners.add(listener);
    }

    public void removeDataFileSizeListener(DataFileSizeListener listener) {
        sizeListeners.remove(listener);
    }

    public String getFileName() {
        return file.getName();
    }

    public File getFile() {
        return file;
    }

    /**
     * Reopens the underlying RandomAccessFile handle. Used after the OS
     * file has been replaced (e.g. post-merge rename).
     */
    public void refresh() throws FileNotFoundException {
        try {
            storeFile.close();
        } catch (IOException e) {
            System.err.println("Error closing file during refresh: " + file.getName());
        }
        storeFile = new RandomAccessFile(file, "rw");
    }

    /**
     * Appends a key-value entry and returns the offset at which it was written.
     * Fires size listeners if the file crosses MAX_FILE_SIZE after the write.
     */
    public synchronized IndexLocation appendEntry(String key, String value)
            throws IOException, NoSuchAlgorithmException {

        storeFile.seek(storeFile.length());

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);
        dos.writeInt(keyBytes.length);
        dos.writeInt(valueBytes.length);
        dos.write(keyBytes);
        dos.write(valueBytes);

        byte[] entryBytes = buf.toByteArray();
        long offset = storeFile.length();
        storeFile.write(entryBytes);

        if (storeFile.length() > MAX_FILE_SIZE) {
            notifySizeListeners();
        }

        writeHeader(storeFile);
        return new IndexLocation(file.getName(), offset);
    }

    /**
     * Same as appendEntry but skips the size-threshold check — used
     * during compaction where the merge file is already sized correctly.
     */
    public synchronized IndexLocation appendEntryWhileMerging(String key, String value)
            throws IOException, NoSuchAlgorithmException {

        storeFile.seek(storeFile.length());

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);
        dos.writeInt(keyBytes.length);
        dos.writeInt(valueBytes.length);
        dos.write(keyBytes);
        dos.write(valueBytes);

        byte[] entryBytes = buf.toByteArray();
        long offset = storeFile.length();
        storeFile.write(entryBytes);

        writeHeader(storeFile);
        return new IndexLocation(file.getName(), offset);
    }

    /** Reads the value for the entry at the given disk offset. */
    public synchronized String readEntry(IndexLocation location) throws IOException {
        storeFile.seek(location.getOffset());
        DataInputStream dis = new DataInputStream(new FileInputStream(storeFile.getFD()));

        int keySize = dis.readInt();
        int valueSize = dis.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];
        dis.read(keyBytes);
        dis.read(valueBytes);

        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    /** Reads the key at the given disk offset (used during index verification). */
    public synchronized String readKey(IndexLocation location) throws IOException {
        storeFile.seek(location.getOffset());
        DataInputStream dis = new DataInputStream(new FileInputStream(storeFile.getFD()));

        int keySize = dis.readInt();
        dis.readInt(); // value size — skip
        byte[] keyBytes = new byte[keySize];
        dis.read(keyBytes);

        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    /** Scans every entry in the file sequentially, used during index rebuild on startup. */
    public synchronized List<Entry> readEntries() throws IOException {
        List<Entry> entries = new ArrayList<>();
        if (file.length() == 0) {
            return entries;
        }

        storeFile.seek(HEADER_SIZE);
        DataInputStream dis = new DataInputStream(new FileInputStream(storeFile.getFD()));
        long offset = HEADER_SIZE;

        while (offset < storeFile.length()) {
            int keySize = dis.readInt();
            int valueSize = dis.readInt();

            byte[] keyBytes = new byte[keySize];
            byte[] valueBytes = new byte[valueSize];
            dis.read(keyBytes);
            dis.read(valueBytes);

            entries.add(new Entry(
                    new String(keyBytes, StandardCharsets.UTF_8),
                    new String(valueBytes, StandardCharsets.UTF_8),
                    offset
            ));

            offset += 8L + keySize + valueSize;
        }
        return entries;
    }

    /**
     * Marks the file as soft-deleted by flipping the state byte in the header.
     * The file stays on disk until the background cleanup task removes it.
     */
    public synchronized boolean softDeleteFile() throws IOException, NoSuchAlgorithmException {
        try {
            updateFileState(storeFile, (byte) 1);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void notifySizeListeners() {
        for (DataFileSizeListener listener : sizeListeners) {
            listener.onFileSizeExceeded(this);
        }
    }
}

package org.niteshnij.core;

import org.niteshnij.io.DataFile;
import org.niteshnij.merger.CompactAndMerge;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main implementation of the key-value store.
 *
 * Writes go to the active segment file and the in-memory index is updated
 * atomically. Reads consult the index first, then seek to the exact byte
 * offset in the correct file.
 *
 * Compaction is single-threaded and guarded by an IDLE/IN_PROGRESS state
 * flag so concurrent triggers are dropped rather than queued.
 */
public class KeyValueStoreImpl implements KeyValueStore {

    private enum MergeState { IDLE, IN_PROGRESS }

    private final DataFilesManager dataFileManager;
    private final ConcurrentHashMap<String, IndexLocation> memoryIndex = new ConcurrentHashMap<>();
    private MergeState mergeState = MergeState.IDLE;

    public KeyValueStoreImpl() throws FileNotFoundException {
        this.dataFileManager = new DataFilesManager();
        loadIndexFromDisk();
    }

    @Override
    public void put(String key, String value) throws IOException, NoSuchAlgorithmException {
        IndexLocation location = dataFileManager.getCurrentDataFile().appendEntry(key, value);
        memoryIndex.put(key, location);
    }

    @Override
    public String get(String key) throws IOException {
        IndexLocation location = memoryIndex.get(key);
        if (location == null) {
            return null;
        }
        DataFile dataFile = dataFileManager.getDataFile(location.getFileName());
        if (dataFile == null) {
            return null;
        }
        return dataFile.readEntry(location);
    }

    @Override
    public void compactAndMerge() {
        synchronized (this) {
            if (mergeState == MergeState.IN_PROGRESS) {
                return;
            }
            mergeState = MergeState.IN_PROGRESS;
        }

        try {
            List<DataFile> files = dataFileManager.getFilesForMerging();
            if (files.size() > 1) {
                CompactAndMerge.merge(memoryIndex, files.get(0), files.get(1), dataFileManager);
            }
        } finally {
            synchronized (this) {
                mergeState = MergeState.IDLE;
            }
        }
    }

    /**
     * Rebuilds the in-memory index by scanning all segment files on disk.
     * Later entries overwrite earlier ones for the same key, which matches
     * the append-only write semantics.
     */
    private void loadIndexFromDisk() {
        try {
            for (DataFile file : dataFileManager.getDataFiles()) {
                for (DataFile.Entry entry : file.readEntries()) {
                    memoryIndex.put(entry.key, new IndexLocation(file.getFileName(), entry.offset));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load index from disk", e);
        }
    }
}

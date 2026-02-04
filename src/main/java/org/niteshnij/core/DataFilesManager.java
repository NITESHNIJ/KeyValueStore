package org.niteshnij.core;

import org.niteshnij.io.DataFile;
import org.niteshnij.listener.DataFileSizeListener;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages the set of live .db segment files.
 *
 * Responsibilities:
 *   - Open existing files on startup (or create the first one)
 *   - Rotate to a new file when the current file exceeds MAX_FILE_SIZE
 *   - Provide file lookup by name for reads
 *   - Run a background task that physically deletes soft-deleted files
 *     after a short grace period (so in-flight reads can still complete)
 */
public class DataFilesManager implements DataFileSizeListener {

    private static final String DATA_DIR = "data";
    // Grace period before a soft-deleted file is physically removed (ms).
    // 2 s is enough for in-flight reads to finish; tune if read latency spikes.
    private static final long DELETE_GRACE_MS = 2000;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private List<DataFile> dataFiles = new ArrayList<>();
    private DataFile currentDataFile;
    private final ScheduledExecutorService scheduler;

    public DataFilesManager() throws FileNotFoundException {
        try {
            File directory = new File(DATA_DIR);
            directory.mkdirs();

            List<File> existingFiles = Arrays.stream(
                    Objects.requireNonNull(directory.listFiles(), "Could not list data directory"))
                    .filter(File::isFile)
                    .filter(f -> f.getName().endsWith(".db"))
                    .collect(Collectors.toList());

            if (!existingFiles.isEmpty()) {
                for (File f : existingFiles) {
                    try {
                        dataFiles.add(new DataFile(f));
                    } catch (Exception e) {
                        System.err.println("Skipping unreadable file: " + f.getName());
                    }
                }
                dataFiles.sort(Comparator.comparing(DataFile::getFileName));
            }

            if (dataFiles.isEmpty()) {
                File newFile = new File(DATA_DIR, Instant.now().toEpochMilli() + ".db");
                newFile.createNewFile();
                DataFile dataFile = new DataFile(newFile);
                dataFiles.add(dataFile);
            }

            currentDataFile = dataFiles.get(dataFiles.size() - 1);
            currentDataFile.addDataFileSizeListener(this);

            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(
                    this::cleanupSoftDeletedFiles,
                    5000,
                    5000,
                    java.util.concurrent.TimeUnit.MILLISECONDS
            );

        } catch (Exception e) {
            throw new RuntimeException("DataFilesManager initialization failed: " + e.getMessage(), e);
        }
    }

    public DataFile getCurrentDataFile() {
        return currentDataFile;
    }

    @Override
    public void onFileSizeExceeded(DataFile dataFile) {
        // Only rotate if the notification is from the currently active file.
        if (dataFile != currentDataFile) {
            return;
        }
        try {
            File newFile = new File(DATA_DIR, Instant.now().toEpochMilli() + ".db");
            DataFile newDataFile = new DataFile(newFile);

            lock.writeLock().lock();
            try {
                dataFiles.add(newDataFile);
            } finally {
                lock.writeLock().unlock();
            }

            currentDataFile = newDataFile;
            currentDataFile.addDataFileSizeListener(this);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to rotate data file", e);
        }
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public void addDataFile(DataFile dataFile) {
        lock.writeLock().lock();
        try {
            dataFiles.add(dataFile);
            dataFiles.sort(Comparator.comparing(DataFile::getFileName));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeDataFile(DataFile dataFile) {
        lock.writeLock().lock();
        try {
            dataFiles.removeIf(f -> f.getFileName().equals(dataFile.getFileName()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns all active (state=0) files eligible for compaction. */
    public List<DataFile> getFilesForMerging() {
        lock.readLock().lock();
        try {
            return dataFiles.stream()
                    .filter(f -> f.getFileState() == 0)
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    public DataFile getDataFile(String fileName) {
        lock.readLock().lock();
        try {
            return dataFiles.stream()
                    .filter(f -> f.getFileName().equals(fileName))
                    .findFirst()
                    .orElse(null);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Physically deletes files that were soft-deleted beyond the grace period. */
    void cleanupSoftDeletedFiles() {
        lock.readLock().lock();
        try {
            for (DataFile file : dataFiles) {
                boolean pastGrace = System.currentTimeMillis() - file.getFile().lastModified() > DELETE_GRACE_MS;
                if (file.getFile().exists() && file.getFileState() == 1 && pastGrace) {
                    file.getFile().delete();
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}

package org.niteshnij.merger;

import org.niteshnij.core.DataFilesManager;
import org.niteshnij.core.IndexLocation;
import org.niteshnij.io.DataFile;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Compacts two segment files into one, handling concurrent reads safely.
 *
 * Race condition strategy:
 *   1. Read both files while they are still live in the DataFilesManager.
 *   2. Write surviving entries (those still current in the main index) to a
 *      new versioned file (timestamp.version.db).
 *   3. Register the new file in the manager *before* updating the index, so
 *      any in-flight reads can still resolve from the old files.
 *   4. Update the main index only for keys whose index still points to one
 *      of the two original files — a newer write that arrived during the
 *      merge will point to a later file and must not be overwritten.
 *   5. Soft-delete the two original files; the background cleanup task in
 *      DataFilesManager physically removes them after a grace period.
 */
public class CompactAndMerge {

    /**
     * Merges dataFile1 and dataFile2 into a single new file.
     *
     * @param memoryIndex    Shared in-memory key → location index
     * @param dataFile1      Older segment
     * @param dataFile2      Newer segment
     * @param filesManager   File registry (add/remove new file, soft-delete old ones)
     */
    public static synchronized void merge(
            ConcurrentHashMap<String, IndexLocation> memoryIndex,
            DataFile dataFile1,
            DataFile dataFile2,
            DataFilesManager filesManager) {

        try {
            List<DataFile.Entry> entries1 = dataFile1.readEntries();
            List<DataFile.Entry> entries2 = dataFile2.readEntries();

            // Keep the lexicographically larger name (later timestamp) as the base for the merged name.
            DataFile baseFile = dataFile1.getFileName().compareTo(dataFile2.getFileName()) > 0
                    ? dataFile1 : dataFile2;

            int version = 0;
            String[] parts = baseFile.getFileName().split("\\.");
            if (parts.length > 1) {
                try {
                    version = Integer.parseInt(parts[1]);
                } catch (NumberFormatException ignored) {
                }
            }

            String mergedName = parts[0] + "." + (version + 1) + ".db";
            File mergedFile = new File("data", mergedName);

            if (!mergedFile.createNewFile()) {
                System.err.println("Merge aborted: could not create " + mergedName);
                return;
            }

            DataFile mergedDataFile = new DataFile(mergedFile);
            ConcurrentHashMap<String, IndexLocation> mergedIndex = new ConcurrentHashMap<>();

            // Copy entries that are still current in the main index.
            for (DataFile.Entry entry : entries1) {
                IndexLocation cur = memoryIndex.get(entry.key);
                if (cur != null && cur.getFileName().equals(dataFile1.getFileName())
                        && cur.getOffset() == entry.offset) {
                    IndexLocation loc = mergedDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    mergedIndex.put(entry.key, new IndexLocation(mergedDataFile.getFileName(), loc.getOffset()));
                }
            }
            for (DataFile.Entry entry : entries2) {
                IndexLocation cur = memoryIndex.get(entry.key);
                if (cur != null && cur.getFileName().equals(dataFile2.getFileName())
                        && cur.getOffset() == entry.offset) {
                    IndexLocation loc = mergedDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    mergedIndex.put(entry.key, new IndexLocation(mergedDataFile.getFileName(), loc.getOffset()));
                }
            }

            // Make the merged file visible before touching the index.
            filesManager.addDataFile(mergedDataFile);

            // Update main index — skip keys that got a newer write during the merge.
            for (Map.Entry<String, IndexLocation> e : mergedIndex.entrySet()) {
                IndexLocation current = memoryIndex.get(e.getKey());
                if (current != null && current.getFileName().compareTo(e.getValue().getFileName()) <= 0) {
                    memoryIndex.put(e.getKey(), e.getValue());
                }
            }

            dataFile1.softDeleteFile();
            dataFile2.softDeleteFile();

        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Merge failed: " + e.getMessage(), e);
        }
    }
}

package org.niteshnij.listener;

import org.niteshnij.io.DataFile;

/**
 * Callback fired when a data file crosses the size threshold.
 * Implemented by DataFilesManager to trigger rotation to a new file.
 */
public interface DataFileSizeListener {
    void onFileSizeExceeded(DataFile dataFile);
}

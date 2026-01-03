package org.niteshnij.core;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Core interface for the key-value store. All operations are designed to be
 * thread-safe when used through the scheduler layer.
 */
public interface KeyValueStore {
    void put(String key, String value) throws IOException, NoSuchAlgorithmException;
    String get(String key) throws IOException;
    void compactAndMerge();
}

# KeyValueStore

A Bitcask-style key-value store built in Java. Uses append-only segment files for fast writes, an in-memory hash index for O(1) reads, and a background compaction process to reclaim disk space.

## How it works

### Write path

Every `put(key, value)` call appends a fixed-format record to the active segment file:

```
[4 bytes] key length (int)
[4 bytes] value length (int)
[N bytes] key (UTF-8)
[M bytes] value (UTF-8)
```

After the write, the in-memory index is updated to point to the new offset. Old values for the same key are not overwritten — they just become unreachable from the index.

### Read path

`get(key)` looks up the key in the in-memory `ConcurrentHashMap`, gets the `(fileName, byteOffset)` location, and seeks directly to that position in the correct segment file. No scan required.

### File rotation

Each segment file is capped at **1 MB**. When a write causes the active file to exceed that threshold, a new file is created (named by epoch milliseconds, e.g. `1736012345678.db`). Old files are kept until compaction runs.

### File format

Every `.db` file starts with a 21-byte header:

```
[0-3]   0x1234ABCD  — magic number
[4]     file state  — 0 = active, 1 = soft-deleted
[5-20]  MD5 checksum of all data bytes after the header
```

### Compaction

When multiple segment files accumulate, compaction merges two of them into one:

1. Both files are read while still live in the file registry.
2. Only entries still referenced by the current index are written to a new merged file.
3. The new file is registered *before* the index is updated, so concurrent reads can still resolve from the old files during the transition.
4. The index is updated — but only for keys that haven't received a newer write during the merge (newer writes point to a file with a later timestamp, and their index entries are left untouched).
5. The two original files are soft-deleted. A background thread physically removes them after a 2-second grace period.

Compaction is triggered stochastically at ~1% of writes, so no dedicated scheduler is needed.

### Concurrency model

| Layer | Threads | Notes |
|---|---|---|
| Writes | 1 | Single writer guarantees append ordering |
| Reads | 5 | Thread pool; `DataFile` methods are synchronized |
| Compaction | 1 (caller) | Guarded by `MergeState` flag; concurrent triggers are dropped |
| File cleanup | 1 (scheduled) | Runs every 5 s in the background |

## Project structure

```
src/main/java/org/niteshnij/
├── Main.java                         # Interactive CLI
├── core/
│   ├── KeyValueStore.java            # Interface
│   ├── KeyValueStoreImpl.java        # Main implementation
│   ├── DataFilesManager.java         # File registry + rotation + cleanup
│   ├── DataFileHeader.java           # Binary header read/write
│   ├── IndexLocation.java            # (fileName, byteOffset) pointer
│   └── StorageEngine.java            # Placeholder for future extensions
├── io/
│   └── DataFile.java                 # Append-only segment file
├── listener/
│   └── DataFileSizeListener.java     # Callback for file size threshold
├── merger/
│   └── CompactAndMerge.java          # Compaction logic
└── scheduler/
    ├── MasterTask.java               # Read/write task coordinator
    ├── ReadTask.java                 # Callable with retry logic
    ├── WriteTask.java                # Runnable write unit
    └── ReadCallBack.java             # Optional callback interface
```

## Running

```bash
mvn package
java -jar target/KeyValueStore-1.0-SNAPSHOT.jar
```

Available commands at the prompt:

```
> put         store a key-value pair
> get         retrieve a value by key (prints "(not found)" if absent)
> merge       manually trigger compaction
> exit        shut down
```

## Requirements

- Java 11+
- Maven 3.x

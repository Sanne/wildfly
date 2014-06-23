/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.as.clustering.infinispan.persistence.file;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.as.clustering.infinispan.persistence.Initializable;

/**
 * A file cache store implementation that uses a file per cache entry.
 *
 * The file cache store included in Infinispan 6+ has some undesirable side effects.
 * <ul>
 * <li>It is prone to OOMs, since cache keys are always stored in memory.</li>
 * <li>The single file that contains all of the cache entries is prone to fragmentation, which requires excessive disk space.</li>
 * <li>The size of the file only ever grows - it never shrinks.</li>
 * <li>Corruption of the single file can easily invalidate all entries in the store, instead of just a single entry.
 * </ul>
 * Since asynchronous passivation is our primary use case for the file cache store, the performance benefits of the SingleFileStore aren't worth the costs.
 * 
 * This implementation is similar to the FileCacheStore found in Infinispan 5.x, but differs in the following ways:
 * <ul>
 * <li>Uses a file per cache entry instead of a file per bucket</li>
 * <li>Keys are encoded into the file path instead of being serialized within the file itself</li>
 * </ul>
 * Consequently, operations like:
 * <ul>
 * <li>{@link #contains(Object)}</li>
 * <li>{@link #delete(Object)}</li>
 * <li>{@link #size()}</li>
 * <li>{@link #process(org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter, org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask, Executor, boolean, boolean)}</li>
 * </ul>
 * are more performant than the bucket-based implementation, since these operations do not require reading and deserialization of file contents.
 *
 * @author Paul Ferraro
 * @param <K> cache key type
 * @param <V> cache value type
 */
@ConfiguredBy(FileStoreConfiguration.class)
public class FileStore<K, V> implements AdvancedLoadWriteStore<K, V> {

    private Path parent;
    private int maxFileNameLength;
    private MarshalledEntryFactory<K, V> entryFactory;
    private ByteBufferFactory bufferFactory;
    private TwoWayKey2StringMapper mapper;

    @Override
    public void init(InitializationContext context) {
        FileStoreConfiguration config = context.getConfiguration();
        this.parent = Paths.get(config.location(), context.getCache().getName());
        this.maxFileNameLength = config.maxFileNameLength();
        this.entryFactory = context.getMarshalledEntryFactory();
        this.bufferFactory = context.getByteBufferFactory();
        this.mapper = config.mapper();
        if (this.mapper instanceof Initializable) {
            ((Initializable) this.mapper).init(context);
        }
    }

    @Override
    public void start() {
        try {
            Files.createDirectories(this.parent);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public MarshalledEntry<K, V> load(Object key) {
        Path path = this.path(key);
        return Files.exists(path) ? this.read(key, path) : null;
    }

    @Override
    public boolean contains(Object key) {
        return Files.exists(this.path(key));
    }

    @Override
    public void write(MarshalledEntry<? extends K, ? extends V> entry) {
        Path path = this.path(entry.getKey());
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC))) {
            ByteBuffer value = entry.getValueBytes();
            ByteBuffer metaData = entry.getMetadataBytes();

            int valueLength = (value != null) ? value.getLength() : 0;
            int metaDataLength = (metaData != null) ? metaData.getLength() : 0;

            output.writeInt(valueLength);
            output.writeInt(metaDataLength);

            if (valueLength > 0) {
                output.write(value.getBuf(), value.getOffset(), value.getLength());
            }
            if (metaDataLength > 0) {
                output.write(metaData.getBuf(), metaData.getOffset(), metaData.getLength());
            }
            output.flush();
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean delete(Object key) {
        try {
            return Files.deleteIfExists(this.path(key));
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void process(final KeyFilter<? super K> filter, final CacheLoaderTask<? super K, ? super V> processor, Executor executor, boolean fetchValue, boolean fetchMetaData) {
        TaskContext context = new TaskContextImpl();
        ExecutorAllCompletionService service = new ExecutorAllCompletionService(executor);
        this.process(this.parent, filter, processor, context, service, !fetchValue && !fetchMetaData);
        service.waitUntilAllCompleted();
        if (service.isExceptionThrown()) {
            throw new PersistenceException(service.getFirstException());
        }
    }

    private <SK, SV> void process(Path parent, KeyFilter<? super K> filter, final CacheLoaderTask<? super K, ? super V> processor, final TaskContext context, CompletionService<Void> service, final boolean keyOnly) {
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(parent)) {
            for (final Path path: directory) {
                if (context.isStopped()) return;
                if (Files.isDirectory(path)) {
                    this.process(path, filter, processor, context, service, keyOnly);
                } else {
                    final K key = FileStore.this.key(path);
                    if (filter.accept(key)) {
                        Runnable task = new Runnable() {
                            @SuppressWarnings("unchecked")
                            @Override
                            public void run() {
                                // The Infinispan API is a little screwy here, so we need to resort to type erasure so the MarshalledEntry is compatible with the CacheLoaderTask
//                              MarshalledEntry<K, V> entry = keyOnly ? FileStore.this.entry(key) : FileStore.this.read(key, path);
                                @SuppressWarnings("rawtypes")
                                MarshalledEntry entry = keyOnly ? FileStore.this.entry(key) : FileStore.this.read(key, path);
                                try {
                                    processor.processEntry(entry, context);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        };
                        service.submit(task, null);
                    }
                }
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public int size() {
        try {
            return this.count(this.parent);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    private int count(Path parent) throws IOException {
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(parent)) {
            int count = 0;
            for (Path path: directory) {
                count += Files.isDirectory(path) ? count(path) : 1;
            }
            return count;
        }
    }

    @Override
    public void clear() {
        try {
            this.clear(this.parent);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    private void clear(Path parent) throws IOException {
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(parent)) {
            for (Path path: directory) {
                if (Files.isDirectory(path)) {
                    this.clear(path);
                }
                Files.delete(path);
            }
        }
    }

    @Override
    public void purge(Executor threadPool, final PurgeListener<? super K> listener) {
        final Path parent = this.parent;
        final long now = System.currentTimeMillis();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                this.purge(parent);
            }

            private void purge(Path parent) {
                try (DirectoryStream<Path> directory = Files.newDirectoryStream(parent)) {
                    for (Path path: directory) {
                        if (Thread.currentThread().isInterrupted()) return;
                        if (Files.isDirectory(path)) {
                            this.purge(path);
                        } else {
                            K key = FileStore.this.key(path);
                            MarshalledEntry<K, V> entry = FileStore.this.read(key, path);
                            InternalMetadata metaData = entry.getMetadata();
                            if ((metaData != null) && metaData.isExpired(now)) {
                                Files.delete(path);
                                listener.entryPurged(key);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
        };
        threadPool.execute(task);
    }

    /*
     * Creates a path from a key.
     * File names are generated from cache keys via URL-friendly base-64 encoding of their externalized form.
     */
    private Path path(Object key) {
        String encoded = this.mapper.getStringMapping(key);
        return this.format(encoded);
    }

    /*
     * Formats the specified value into a path, using sub-directories if necessary.
     */
    private Path format(String value) {
        int size = value.length();
        if (size <= this.maxFileNameLength) {
            return this.parent.resolve(value);
        }
        int directories = (size / this.maxFileNameLength) - (((size % this.maxFileNameLength) == 0) ? 1 : 0);
        Path directory = this.parent;
        for (int i = 0; i < directories; ++i) {
            directory = directory.resolve(value.substring(i * this.maxFileNameLength, (i + 1) * this.maxFileNameLength));
            try {
                Files.createDirectory(directory);
            } catch (FileAlreadyExistsException e) {
                // Ignore
            } catch (IOException e) {
                throw new PersistenceException(e);
            }
        }
        return directory.resolve(value.substring(directories * this.maxFileNameLength));
    }

    /*
     * Extracts the key from a path.
     */
    @SuppressWarnings("unchecked")
    K key(Path path) {
        String encoded = this.parse(path);
        return (K) this.mapper.getKeyMapping(encoded);
    }

    /*
     * Parses the specified path, potentially containing sub-directories.
     */
    private String parse(Path path) {
        Path relative = this.parent.relativize(path);
        String fileName = relative.getFileName().toString();
        int directories = relative.getNameCount() - 1;
        if (directories == 0) {
            return fileName;
        }
        StringBuilder builder = new StringBuilder((directories * this.maxFileNameLength) + fileName.length());
        for (Path name: relative) {
            builder.append(name);
        }
        return builder.toString();
    }

    /*
     * Reads the content of the specified file that corresponds to the specified key
     */
    MarshalledEntry<K, V> read(Object key, Path path) {
        try (DataInputStream input = new DataInputStream(Files.newInputStream(path, StandardOpenOption.READ))) {
            int valueLength = input.readInt();
            int metaDataLength = input.readInt();

            byte[] bytes = new byte[valueLength + metaDataLength];
            input.readFully(bytes);

            ByteBuffer valueBuffer = (valueLength > 0) ? FileStore.this.bufferFactory.newByteBuffer(bytes, 0, valueLength) : null;
            ByteBuffer metaDataBuffer = (metaDataLength > 0) ? FileStore.this.bufferFactory.newByteBuffer(bytes, valueLength, metaDataLength) : null;
            return FileStore.this.entryFactory.newMarshalledEntry(key, valueBuffer, metaDataBuffer);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    MarshalledEntry<K, V> entry(K key) {
        return this.entryFactory.newMarshalledEntry(key, (ByteBuffer) null, (ByteBuffer) null);
    }
}

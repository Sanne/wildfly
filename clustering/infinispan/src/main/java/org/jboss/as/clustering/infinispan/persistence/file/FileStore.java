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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.TimeService;
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
 * @author Sanne Grinovero
 * @param <K> cache key type
 * @param <V> cache value type
 */
@ConfiguredBy(FileStoreConfiguration.class)
public class FileStore<K, V> implements AdvancedLoadWriteStore<K, V> {

    private Path parent;
    private MarshalledEntryFactory<K, V> entryFactory;
    private ByteBufferFactory bufferFactory;
    private TwoWayKey2StringMapper mapper;
    private IndexBasedStorage<K, V> storage;
    private TimeService timeService;

    @Override
    public void init(InitializationContext context) {
        FileStoreConfiguration config = context.getConfiguration();
        timeService = context.getTimeService();
        this.parent = Paths.get(config.location(), context.getCache().getName());
        this.entryFactory = context.getMarshalledEntryFactory();
        this.bufferFactory = context.getByteBufferFactory();
        this.mapper = config.mapper();
        this.storage = new IndexBasedStorage(entryFactory, bufferFactory, mapper);
        if (this.mapper instanceof Initializable) {
            ((Initializable) this.mapper).init(context);
        }
    }

    @Override
    public void start() {
        try {
            Path createdDirectory = Files.createDirectories(this.parent);
            storage.start(createdDirectory);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void stop() {
        try {
            storage.stop();
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public MarshalledEntry<K, V> load(Object key) {
        String name = this.path(key);
        try {
            return storage.load(key, name);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean contains(Object key) {
        try {
            return storage.contains(this.path(key));
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void write(MarshalledEntry<? extends K, ? extends V> entry) {
        try {
            storage.write(entry.getKey(), entry.getValueBytes(), entry.getMetadata(), entry.getMetadataBytes());
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean delete(Object key) {
        final String name = this.path(key);
        try {
            boolean returnValue = storage.contains(name);
            storage.delete(name);
            //Do we need a reliable return value?
            //I suspect we could skip the contains operation.
            return returnValue;
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void process(final KeyFilter<? super K> filter, final CacheLoaderTask<? super K, ? super V> processor, Executor executor, boolean fetchValue, boolean fetchMetaData) {
        TaskContext context = new TaskContextImpl();
        ExecutorAllCompletionService service = new ExecutorAllCompletionService(executor);
        final long now = timeService.wallClockTime();
        try {
            storage.process(filter, processor, context, service, !fetchValue && !fetchMetaData, now);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
        service.waitUntilAllCompleted();
        if (service.isExceptionThrown()) {
            throw new PersistenceException(service.getFirstException());
        }
    }

    @Override
    public int size() {
        try {
            return storage.count();
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void clear() {
        try {
            storage.removeAll();
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void purge(Executor threadPool, final PurgeListener<? super K> listener) {
        final IndexBasedStorage<K,V> storage = this.storage;
        final long now = timeService.wallClockTime();
        final Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    storage.purgeAll(now, listener);
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
        };
        threadPool.execute(task);
    }

    /*
     * Creates a String version of the key.
     * Names are generated from cache keys via URL-friendly base-64 encoding of their externalized form.
     */
    private String path(final Object key) {
        return this.mapper.getStringMapping(key);
    }

    /*
     * Extracts the key from a path.
     */
    @SuppressWarnings("unchecked")
    K key(final String encoded) {
        return (K) this.mapper.getKeyMapping(encoded);
    }

}

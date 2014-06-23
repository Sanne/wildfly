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

package org.jboss.as.clustering.infinispan.persistence;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedCacheWriter.PurgeListener;
import org.infinispan.persistence.spi.InitializationContext;
import org.jboss.as.clustering.infinispan.CacheContainer;
import org.jboss.as.clustering.infinispan.persistence.file.FileStore;
import org.jboss.as.clustering.infinispan.persistence.file.FileStoreConfiguration;
import org.jboss.as.clustering.infinispan.persistence.file.FileStoreConfigurationBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link FileStore}.
 * @author Paul Ferraro
 */
public class FileStoreTestCase {
    @SuppressWarnings("unchecked")
    private final MarshalledEntryFactory<String, UUID> entryFactory = new MarshalledEntryFactoryImpl(new GenericJBossMarshaller());
    private final FileStore<String, UUID> store = new FileStore<>();

    @Before
    public void init() throws IOException {
        Path parent = Files.createTempDirectory(null);
        parent.toFile().deleteOnExit();
        InitializationContext context = mock(InitializationContext.class);
        Cache<String, UUID> cache = mock(Cache.class);
        FileStoreConfiguration config = new ConfigurationBuilder().persistence().addStore(FileStoreConfigurationBuilder.class)
                .location(parent.toString())
                .mapper(new DefaultTwoWayKey2StringMapper())
                .maxFileNameLength(10)
                .create();

        when(context.getCache()).thenReturn(cache);
        when(cache.getName()).thenReturn(CacheContainer.DEFAULT_CACHE_ALIAS);
        when(context.getConfiguration()).thenReturn(config);
        when(context.getByteBufferFactory()).thenReturn(new ByteBufferFactoryImpl());
        when(context.getMarshalledEntryFactory()).thenReturn(this.entryFactory);

        this.store.init(context);
        this.store.start();
    }

    @After
    public void destroy() {
        this.store.stop();
    }

    @Test
    public void shortKey() {
        this.test(5);
    }

    @Test
    public void maxNameKey() {
        this.test(10);
    }

    @Test
    public void largeKey() {
        // This should force the file into subdirectories
        this.test(15);
    }

    public void test(int size) {
        char[] chars = new char[size];
        Arrays.fill(chars, '0');
        String key = new String(chars);
        UUID value = UUID.randomUUID();
        InternalMetadata metaData = new SimpleMetaData(false);
        Executor executor = new Executor() {
            @Override
            public void execute(Runnable task) {
                task.run();
            }
        };
        PurgeListener<String> listener = mock(PurgeListener.class);

        this.store.write(this.entryFactory.newMarshalledEntry(key, value, metaData));

        MarshalledEntry<String, UUID> result = this.store.load(key);

        assertEquals(key, result.getKey());
        assertEquals(value, result.getValue());
        assertEquals(metaData, result.getMetadata());

        // Entry should not be purged, because it doesn't expire
        this.store.purge(executor, listener);

        verify(listener, never()).entryPurged(eq(key));

        this.store.write(this.entryFactory.newMarshalledEntry(key, value, null));

        result = this.store.load(key);

        assertEquals(key, result.getKey());
        assertEquals(value, result.getValue());
        assertNull(result.getMetadata());

        // Entry should not be purged, because it has no metadata
        this.store.purge(executor, listener);

        verify(listener, never()).entryPurged(eq(key));

        // Validate contains(...), delete(...)
        assertTrue(this.store.contains(key));
        assertTrue(this.store.delete(key));

        assertFalse(this.store.contains(key));
        assertFalse(this.store.delete(key));

        this.store.write(this.entryFactory.newMarshalledEntry(key, value, new SimpleMetaData(true)));

        assertTrue(this.store.contains(key));

        // Entry should be purged, because it is expired
        this.store.purge(executor, listener);

        verify(listener).entryPurged(eq(key));
    }

    public static class SimpleMetaData implements InternalMetadata, Serializable {
        private static final long serialVersionUID = 7795134829094430277L;

        private final boolean expired;

        SimpleMetaData(boolean expired) {
            this.expired = expired;
        }

        @Override
        public long lifespan() {
            return 0;
        }

        @Override
        public long maxIdle() {
            return 0;
        }

        @Override
        public EntryVersion version() {
            return null;
        }

        @Override
        public Builder builder() {
            return null;
        }

        @Override
        public long created() {
            return 0;
        }

        @Override
        public long lastUsed() {
            return 0;
        }

        @Override
        public boolean isExpired(long now) {
            return this.expired;
        }

        @Override
        public long expiryTime() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}

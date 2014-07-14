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
import java.nio.file.Path;
import java.util.concurrent.CompletionService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask;
import org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext;
import org.infinispan.persistence.spi.AdvancedCacheWriter.PurgeListener;

/**
 * 
 * @author Sanne Grinovero
 */
public class IndexBasedStorage<K, V> {

    /**
     * This could be switched to false for improved performance, but doing
     * so we might still return matches from deleted entries.
     * TODO Would that be acceptable? 
     */
    private static final boolean ALWAYS_FLUSH_DELETES = true;

    private FSDirectory luceneDirectory;
    private IndexWriter indexWriter;
    private final AtomicReference<IndexReader> latestReader = new AtomicReference<>();

    private final MarshalledEntryFactory<K, V> entryFactory;
    private final ByteBufferFactory bufferFactory;
    private final DocumentMapper mapper;

    public IndexBasedStorage(MarshalledEntryFactory<K, V> entryFactory, ByteBufferFactory bufferFactory, TwoWayKey2StringMapper key2stringMapper) {
        this.entryFactory = entryFactory;
        this.bufferFactory = bufferFactory;
        this.mapper = new DocumentMapper(key2stringMapper, bufferFactory, entryFactory);
    }

    public void stop() throws IOException {
        indexWriter.close(false);
        luceneDirectory.close();
    }

    /**
     * Create a new Lucene index, using the passed directory as storage location.
     * @param directory
     * @throws IOException
     */
    public void start(final Path directory) throws IOException {
        final IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_36, new KeywordAnalyzer());
        writerConfig.setOpenMode(OpenMode.CREATE); //Destroy existing index if any is found
        luceneDirectory = FSDirectory.open(directory.toFile());
        indexWriter = new IndexWriter(luceneDirectory, writerConfig);
        IndexReader indexReader = IndexReader.open(indexWriter, ALWAYS_FLUSH_DELETES);
        latestReader.set(indexReader);
    }

    public void removeAll() throws IOException {
        indexWriter.deleteAll();
        indexWriter.commit();
    }

    public int count() throws IOException {
        try (IndexReader ir = getIndexReader()) {
            return ir.numDocs();
        }
    }

    private IndexReader getIndexReader() throws IOException {
        final IndexReader oldReader = latestReader.get();
        oldReader.incRef();
        final IndexReader ifChanged = IndexReader.openIfChanged(oldReader, indexWriter, ALWAYS_FLUSH_DELETES);
        if (ifChanged == null) {
            return oldReader;
        } else {
            latestReader.lazySet(ifChanged);
            oldReader.decRef();
            return ifChanged;
        }
    }

    public void delete(String key) throws IOException {
        indexWriter.deleteDocuments(mapper.documentId(key));
    }

    public boolean contains(String key) throws IOException {
        try (IndexReader ir = getIndexReader()) {
            int frequency = ir.docFreq(mapper.documentId(key));
            return frequency > 0;
        }
    }

    public void purgeAll(long timestamp, PurgeListener<? super K> listener) throws IOException {
        Query expiredEntriesQuery = mapper.makeExpiredEntriesQuery(timestamp);
        Collector purgingCollector = new EntryPurgingCollector(listener, indexWriter, mapper);
        processEachEntry(expiredEntriesQuery, purgingCollector);
    }

    public <SK, SV> void process(KeyFilter<? super K> filter, final CacheLoaderTask<? super K, ? super V> processor, final TaskContext context, CompletionService<Void> service, final boolean keyOnly, final long timestamp) throws IOException {
        Query expiredEntriesQuery = mapper.makeLiveEntriesQuery(timestamp);
        Collector processingCollector = new EntryProcessingCollector(filter, processor, service, context, keyOnly, mapper);
        processEachEntry(expiredEntriesQuery, processingCollector);
    }

    public void write(K key, ByteBuffer valueBytes, InternalMetadata internalMetadata, ByteBuffer serializedMetadata) throws IOException {
        indexWriter.addDocument(mapper.createWriteDocument(key, valueBytes, internalMetadata, serializedMetadata));
    }

    public MarshalledEntry<K, V> load(Object key, String encodedKey) throws IOException {
        try (IndexReader ir = getIndexReader()) {
            final int docId;
            try (TermDocs termDocs = ir.termDocs(mapper.documentId(encodedKey))) {
                termDocs.next();
                docId = termDocs.doc();
            }
            if (ir.isDeleted(docId)) {
                return null;
            }
            final Document match = ir.document(docId);
            return mapper.extractFromDocument(key, match);
        }
    }

    private void processEachEntry(final Query matchesToProcess, final Collector applyEach) throws IOException {
        try (IndexReader ir = getIndexReader()) {
            try (IndexSearcher searcher = new IndexSearcher(ir)) {
                searcher.search(matchesToProcess, applyEach);
            }
        }
    }

}

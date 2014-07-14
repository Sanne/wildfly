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
import java.util.concurrent.CompletionService;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask;
import org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext;

final class EntryProcessingCollector extends Collector {

    private final KeyFilter keyFilter;
    private final CacheLoaderTask processor;
    private final TaskContext context;
    private final boolean keyOnly;
    private final DocumentMapper mapper;

    private IndexReader currentReader;
    private CompletionService<Void> service;

    public EntryProcessingCollector(KeyFilter filter, CacheLoaderTask processor, CompletionService<Void> service, TaskContext context, boolean keyOnly, DocumentMapper mapper) {
        this.keyFilter = filter;
        this.processor = processor;
        this.service = service;
        this.context = context;
        this.keyOnly = keyOnly;
        this.mapper = mapper;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        //Not used
    }

    @Override
    public void collect(final int docId) throws IOException {
        if (context.isStopped()) return;
        final MarshalledEntry entry = mapper.extractMarshalledEntry(currentReader, docId, keyOnly);
        if (keyFilter.accept(entry.getKey())) {
            process(entry);
        }
    }

    private void process(final MarshalledEntry entry) {
        Runnable task = new Runnable() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                try {
                    processor.processEntry(entry, context);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    context.stop();
                }
            }
        };
        service.submit(task, null);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        this.currentReader = reader;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

}

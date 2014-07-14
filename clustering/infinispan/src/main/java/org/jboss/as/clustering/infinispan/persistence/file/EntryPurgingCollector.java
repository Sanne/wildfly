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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.infinispan.persistence.spi.AdvancedCacheWriter.PurgeListener;

final class EntryPurgingCollector<T> extends Collector {

    private final PurgeListener listener;
    private final IndexWriter indexWriter;
    private final DocumentMapper mapper;

    private IndexReader currentReader;

    public EntryPurgingCollector(PurgeListener<T> listener, IndexWriter indexWriter, DocumentMapper mapper) {
        this.listener = listener;
        this.indexWriter = indexWriter;
        this.mapper = mapper;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        //not used
    }

    @SuppressWarnings("unchecked")
    @Override
    public void collect(int doc) throws IOException {
        Document document = currentReader.document(doc, IndexFieldNames.ID_FIELD_SELECTOR);
        String stringEncodedKey = document.get(IndexFieldNames.KEY_FIELD.field());
        indexWriter.deleteDocuments(IndexFieldNames.KEY_FIELD.createTerm(stringEncodedKey));
        listener.entryPurged(mapper.getKeyMapping(stringEncodedKey));
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

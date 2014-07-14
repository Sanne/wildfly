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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;

final class DocumentMapper<K, V> implements IndexFieldNames {

    private TwoWayKey2StringMapper key2stringMapper;
    private ByteBufferFactory bufferFactory;
    private MarshalledEntryFactory<K, V> entryFactory;

    public DocumentMapper(TwoWayKey2StringMapper key2stringMapper, ByteBufferFactory bufferFactory, MarshalledEntryFactory<K, V> entryFactory) {
        this.key2stringMapper = key2stringMapper;
        this.bufferFactory = bufferFactory;
        this.entryFactory = entryFactory;
    }

    MarshalledEntry<K, V> extractFromDocument(K key, Document match) {
        byte[] valueBinary = match.getBinaryValue(VALUE_FIELD.field());
        byte[] metaDataBinary = match.getBinaryValue(METADATA_FIELD.field());
        ByteBuffer valueBuffer = (valueBinary != null) ? bufferFactory.newByteBuffer(valueBinary, 0, valueBinary.length) : null;
        ByteBuffer metaDataBuffer = (metaDataBinary != null) ? bufferFactory.newByteBuffer(metaDataBinary, 0, metaDataBinary.length) : null;
        return entryFactory.newMarshalledEntry(key, valueBuffer, metaDataBuffer);
    }

    /**
     * Create a new MarshalledEntry which only has the key
     * @param key
     * @return
     */
    MarshalledEntry<K, V> makeNullEntry(K key) {
        return entryFactory.newMarshalledEntry(key, (ByteBuffer)null, (ByteBuffer)null);
    }

    Document createWriteDocument(K key, ByteBuffer valueBytes, InternalMetadata internalMetadata, ByteBuffer serializedMetadata) throws IOException {
        final String name = encodeKey2String(key);
        Document storedDocument = new Document();
        storedDocument.add(new Field(KEY_FIELD.field(), false, name, Store.NO, Index.NOT_ANALYZED_NO_NORMS, TermVector.NO));
        storedDocument.add(new Field(VALUE_FIELD.field(), valueBytes.getBuf()));
        if (serializedMetadata!=null) {
            storedDocument.add(new Field(METADATA_FIELD.field(), serializedMetadata.getBuf()));
        }
        if (internalMetadata!=null) {
            NumericField expiryField = new NumericField(EXPIRY_FIELD.field(), NumericUtils.PRECISION_STEP_DEFAULT, Field.Store.NO, true);
            expiryField.setLongValue(internalMetadata.expiryTime());
            storedDocument.add(expiryField);
        }
        return storedDocument;
    }

    Term documentId(String key) {
        //reuses interning of the Field name
        return KEY_FIELD.createTerm(key);
    }

    K getKeyMapping(String encodedKey) {
        return (K) key2stringMapper.getKeyMapping(encodedKey);
    }

    String encodeKey2String(K key) {
        return key2stringMapper.getStringMapping(key);
    }

    Query makeExpiredEntriesQuery(long timestamp) {
        return NumericRangeQuery.newLongRange(EXPIRY_FIELD.field(), null, Long.valueOf(timestamp), false, false);
    }

    Query makeLiveEntriesQuery(long timestamp) {
        return NumericRangeQuery.newLongRange(EXPIRY_FIELD.field(), Long.valueOf(timestamp), null, true, false);
    }

    public K extractKeyExclusive(IndexReader reader, final int docId) throws IOException {
        final Document document = reader.document(docId, IndexFieldNames.ID_FIELD_SELECTOR);
        return keyFromDocument(document);
    }

    private K keyFromDocument(final Document document) {
        final String stringEncodedKey = document.get(IndexFieldNames.KEY_FIELD.field());
        final K key = getKeyMapping(stringEncodedKey);
        return key;
    }

    public MarshalledEntry extractMarshalledEntry(final IndexReader reader, final int docId, final boolean keyOnly) throws IOException {
        if (keyOnly) {
            K key = extractKeyExclusive(reader, docId);
            return makeNullEntry(key);
        }
        else {
            final Document document = reader.document(docId);
            return extractFromDocument(keyFromDocument(document), document);
        }
    }

}

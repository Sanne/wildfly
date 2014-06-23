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

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.InitializationContext;

/**
 * {@link TwoWayKey2StringMapper} that uses a {@link Codec} to encode/decode keys to/from strings.
 * @author Paul Ferraro
 * @param <K> the key type
 */
public class CodecKeyMapper<K> implements TwoWayKey2StringMapper, Initializable {

    private final Codec codec;
    private MarshalledEntryFactory<K, ?> entryFactory;
    private ByteBufferFactory bufferFactory;

    public CodecKeyMapper(Codec codec) {
        this.codec = codec;
    }

    @Override
    public void init(InitializationContext context) {
        this.entryFactory = context.getMarshalledEntryFactory();
        this.bufferFactory = context.getByteBufferFactory();
    }

    @Override
    public boolean isSupportedType(Class<?> keyType) {
        return true;
    }

    @Override
    public String getStringMapping(Object key) {
        ByteBuffer buffer = this.entryFactory.newMarshalledEntry(key, (ByteBuffer) null, (ByteBuffer) null).getKeyBytes();
        byte[] decoded = java.nio.ByteBuffer.allocate(buffer.getLength()).put(buffer.getBuf(), buffer.getOffset(), buffer.getLength()).array();
        return this.codec.encode(decoded);
    }

    @Override
    public K getKeyMapping(String stringKey) {
        byte[] decoded = this.codec.decode(stringKey);
        ByteBuffer keyBuffer = this.bufferFactory.newByteBuffer(decoded, 0, decoded.length);
        return this.entryFactory.newMarshalledEntry(keyBuffer, (ByteBuffer) null, (ByteBuffer) null).getKey();
    }
}

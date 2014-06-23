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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.BaseNCodec;
import org.apache.commons.codec.binary.Hex;
import org.infinispan.persistence.spi.PersistenceException;

public enum Codecs implements Codec {
    /**
     * Provides familiar hex encoding
     */
    HEX() {
        @Override
        public String encode(byte[] bytes) {
            return Hex.encodeHexString(bytes);
        }

        @Override
        public byte[] decode(String value) {
            try {
                return Hex.decodeHex(value.toCharArray());
            } catch (DecoderException e) {
                throw new PersistenceException(e);
            }
        }
    },
    /**
     * Provides longer encoding than {@link #BASE64}, but is safe for use by case-insensitive filesystems.
     */
    BASE32() {
        private final BaseNCodec codec = new Base32(true);

        @Override
        public String encode(byte[] bytes) {
            return this.codec.encodeAsString(bytes);
        }

        @Override
        public byte[] decode(String value) {
            return this.codec.decode(value);
        }
    },
    /**
     * Provides the shortest encoding, but is not safe for use by case-insensitive filesystems.
     */
    BASE64() {
        @Override
        public String encode(byte[] bytes) {
            return Base64.encodeBase64URLSafeString(bytes);
        }

        @Override
        public byte[] decode(String value) {
            return Base64.decodeBase64(value);
        }
    };
}

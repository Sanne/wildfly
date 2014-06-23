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

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.jboss.as.clustering.infinispan.persistence.CodecKeyMapper;
import org.jboss.as.clustering.infinispan.persistence.Codecs;

public class FileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<FileStoreConfiguration, FileStoreConfigurationBuilder> {

    private String location = "Infinispan-FileStore";
    private TwoWayKey2StringMapper mapper = new CodecKeyMapper<>(Codecs.BASE32);
    private int maxFileNameLength = Byte.MAX_VALUE - Byte.MIN_VALUE;

    public FileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder);
    }

    public FileStoreConfigurationBuilder location(String location) {
        this.location = location;
        return this;
    }

    public FileStoreConfigurationBuilder mapper(TwoWayKey2StringMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public FileStoreConfigurationBuilder maxFileNameLength(int maxFileNameLength) {
        this.maxFileNameLength = maxFileNameLength;
        return this;
    }

    @Override
    public FileStoreConfiguration create() {
        return new FileStoreConfiguration(this.location, this.mapper, this.maxFileNameLength, this.purgeOnStartup, this.fetchPersistentState, this.ignoreModifications, this.async.create(), this.singletonStore.create(), this.preload, this.shared, this.properties);
    }

    @Override
    public FileStoreConfigurationBuilder self() {
        return this;
    }

    @Override
    public FileStoreConfigurationBuilder read(FileStoreConfiguration template) {
        this.location = template.location();
        super.read(template);
        return this;
    }
}

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

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.Term;

/**
 * This FieldSelector makes sure we only load a single field from
 * a Lucene Document when reading it from the index.
 *
 * @author Sanne Grinovero
 */
final class SingleFieldSelector implements FieldSelector {

    private final String fieldName;

    public SingleFieldSelector(Term term) {
        this.fieldName = term.field();
    }

    @Override
    public FieldSelectorResult accept(String fieldName) {
        if (this.fieldName.equals(fieldName)) {
            return FieldSelectorResult.LOAD_AND_BREAK;
        }
        return FieldSelectorResult.NO_LOAD;
    }

}

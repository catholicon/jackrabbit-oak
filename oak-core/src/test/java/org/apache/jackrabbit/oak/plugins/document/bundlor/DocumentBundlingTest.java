/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertTrue;

public class DocumentBundlingTest {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private DocumentNodeStore store;

    @Before
    public void setUpBundlor() throws CommitFailedException {
        store = builderProvider.newBuilder().getNodeStore();
        NodeState registryState = BundledTypesRegistry.builder()
                .forType("nt:file", "jcr:content")
                .registry()
                .forType("app:Asset", "jcr:content", "jcr:content/metadata")
                .build();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("jcr:system").child("documentstore").setChildNode("bundlor", registryState);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void saveAndReadNtFile() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        System.out.println(store.getDocumentStore().find(Collection.NODES, "2:/test/book.jpg").asString());

        NodeState root = store.getRoot();
        NodeState fileNodeState = root.getChildNode("test");
        assertTrue(fileNodeState.getChildNode("book.jpg").exists());
        assertTrue(fileNodeState.getChildNode("book.jpg").getChildNode("jcr:content").exists());

        assertTrue(PartialEqualsDiff.equals(fileNode.getNodeState(), fileNodeState.getChildNode("book.jpg")));
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static class PartialEqualsDiff extends EqualsDiff {
        private final Set<String> ignoredProps = Sets.newHashSet(DocumentBundlor.META_PROP_PATTERN);

        public static boolean equals(NodeState before, NodeState after) {
            return before.exists() == after.exists()
                    && after.compareAgainstBaseState(before, new PartialEqualsDiff());
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (ignore(after)) return true;
            return super.propertyAdded(after);
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (ignore(after)) return true;
            return super.propertyChanged(before, after);
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (ignore(before)) return true;
            return super.propertyDeleted(before);
        }

        private boolean ignore(PropertyState state){
            return ignoredProps.contains(state.getName());
        }
    }
}

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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class BundlingConfigHandler implements Observer, Closeable {
    private static final String CONFIG_PATH = "/jcr:system/documentstore/bundlor";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private NodeState root = EMPTY_NODE;
    private BackgroundObserver backgroundObserver;
    private Closeable observerRegistration;

    private volatile BundledTypesRegistry registry = BundledTypesRegistry.NOOP;

    private Editor changeDetector = new SubtreeEditor(new DefaultEditor() {
        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            recreateRegistry(after);
        }
    }, Iterables.toArray(PathUtils.elements(CONFIG_PATH), String.class));

    @Override
    public synchronized void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        EditorDiff.process(changeDetector, this.root, root);
        this.root = root;
    }

    public BundlingHandler newBundlingHandler() {
        return new BundlingHandler(registry);
    }

    public void initialize(NodeStore nodeStore, Executor executor) {
        registerObserver(nodeStore, executor);
    }

    @Override
    public void close() throws IOException{
        if (backgroundObserver != null){
            observerRegistration.close();
            backgroundObserver.close();
        }
    }

    BundledTypesRegistry getRegistry() {
        return registry;
    }

    private void recreateRegistry(NodeState nodeState) {
        //TODO Any sanity checks
        registry = BundledTypesRegistry.from(nodeState);
        log.info("Refreshing the BundledTypesRegistry");
    }

    private void registerObserver(NodeStore nodeStore, Executor executor) {
        if (nodeStore instanceof Observable) {
            backgroundObserver = new BackgroundObserver(this, executor, 5);
            observerRegistration = ((Observable) nodeStore).addObserver(backgroundObserver);
        }
    }

}



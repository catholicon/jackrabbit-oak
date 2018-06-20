/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState;
import org.apache.jackrabbit.oak.plugins.migration.report.LoggingReporter;
import org.apache.jackrabbit.oak.plugins.migration.report.ReportingNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfo;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class InitialContentMigrator {

    private static final int LOG_NODE_COPY = Integer.getInteger("oak.upgrade.logNodeCopy", 10000);

    private static final String CLUSTER_ID = System.getProperty("oak.composite.seed.clusterId", "1");

    private static final Logger LOG = LoggerFactory.getLogger(InitialContentMigrator.class);

    private final NodeStore targetNodeStore;

    private final NodeStore seedNodeStore;

    private final Mount seedMount;

    private final Set<String> includePaths;

    private final Set<String> excludePaths;

    private final Set<String> fragmentPaths;

    private final Set<String> excludeFragments;

    public InitialContentMigrator(NodeStore targetNodeStore, NodeStore seedNodeStore, Mount seedMount) {
        this.targetNodeStore = targetNodeStore;
        this.seedNodeStore = seedNodeStore;
        this.seedMount = seedMount;

        this.includePaths = FilteringNodeState.ALL;
        this.excludeFragments = ImmutableSet.of(seedMount.getPathFragmentName());

        if (seedMount instanceof MountInfo) {
            this.excludePaths = ((MountInfo) seedMount).getIncludedPaths();
            this.fragmentPaths = new HashSet<>();
            for (String p : ((MountInfo) seedMount).getPathsSupportingFragments()) {
                fragmentPaths.add(stripPatternCharacters(p));
            }
        } else {
            this.excludePaths = FilteringNodeState.NONE;
            this.fragmentPaths = FilteringNodeState.ALL;
        }
    }

    private boolean isTargetInitialized() {
        return targetNodeStore.getRoot().hasChildNode(":composite");
    }

    private void waitForInitialization() throws IOException {
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } while (!isTargetInitialized());
    }

    public void migrate() throws IOException, CommitFailedException {
        if (isTargetInitialized()) {
            LOG.info("The target is already initialized, no need to copy the seed mount");
        } else if (targetNodeStore instanceof Clusterable) {
            Clusterable dns = (Clusterable) targetNodeStore;
            String clusterId = dns.getInstanceId();
            LOG.info("The target isn't initialized and the cluster id = {}.", clusterId);
            if (CLUSTER_ID.equals(clusterId)) {
                LOG.info("This cluster id {} is configured to initialized the repository.", CLUSTER_ID);
                doMigrate();
            } else {
                LOG.info("Waiting until the repository is initialized by instance {}.", CLUSTER_ID);
                waitForInitialization();
            }
        } else {
            LOG.info("Initializing the default mount.");
            doMigrate();
        }
    }

    protected void doMigrate() throws CommitFailedException {
        LOG.info("Seed {}", seedMount.getName());
        LOG.info("Include: {}", includePaths);
        LOG.info("Exclude: {}", excludePaths);
        LOG.info("Exclude fragments: {} @ {}", excludeFragments, fragmentPaths);

        NodeState targetRoot = targetNodeStore.getRoot();
        NodeBuilder targetBuilder = targetRoot.builder();
        NodeState seedRoot = wrapNodeState(seedNodeStore.getRoot(), true);
        seedRoot.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, new ApplyDiff(targetBuilder));
        targetNodeStore.merge(targetBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String fullTextAsyncId = targetNodeStore.checkpoint(Long.MAX_VALUE, Collections.singletonMap("name", "fulltext-async"));
        String asyncId = targetNodeStore.checkpoint(Long.MAX_VALUE, Collections.singletonMap("name", "async"));

        targetBuilder = targetRoot.builder();
        targetBuilder.getChildNode(":async").remove();
        NodeBuilder asyncNode = targetBuilder.child(":async");
        asyncNode.setProperty("fulltext-async", fullTextAsyncId);
        asyncNode.setProperty("async", asyncId);

        targetNodeStore.merge(targetBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        markMigrationAsDone();
    }

    private void markMigrationAsDone() throws CommitFailedException {
        NodeState root = targetNodeStore.getRoot();
        NodeBuilder builder = root.builder();
        builder.child(":composite");
        targetNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeState wrapNodeState(NodeState nodeState, boolean logPaths) {
        NodeState wrapped = nodeState;
        wrapped = FilteringNodeState.wrap("/", wrapped, includePaths, excludePaths, fragmentPaths, excludeFragments);
        if (logPaths) {
            wrapped = ReportingNodeState.wrap(wrapped, new LoggingReporter(LOG, "Copying", LOG_NODE_COPY, -1));
        }
        return wrapped;
    }

    private static String stripPatternCharacters(String pathPattern) {
        String result = pathPattern;
        result = substringBefore(result, '*');
        result = substringBefore(result, '$');
        if (!result.equals(pathPattern)) {
            int slashIndex = result.lastIndexOf('/');
            if (slashIndex > 0) {
                result = result.substring(0, slashIndex);
            }
        }
        return result;
    }

    private static String substringBefore(String subject, char stopCharacter) {
        int index = subject.indexOf(stopCharacter);
        if (index > -1) {
            return subject.substring(0, index);
        } else {
            return subject;
        }
    }

}

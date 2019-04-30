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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchCoordinateFactory;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ElasticsearchIndexProvider implements QueryIndexProvider {
    private final ElasticsearchCoordinateFactory esCoordFactory;

    public ElasticsearchIndexProvider(@NotNull ElasticsearchCoordinateFactory esCoordFactory) {
        this.esCoordFactory = esCoordFactory;
    }

    @Override
    public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return ImmutableList.of(new ElasticsearchIndex(esCoordFactory, nodeState));
    }
}

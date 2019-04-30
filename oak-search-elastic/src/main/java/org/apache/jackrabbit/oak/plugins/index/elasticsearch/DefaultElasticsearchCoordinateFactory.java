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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Map;

public class DefaultElasticsearchCoordinateFactory implements ElasticsearchCoordinateFactory {
    private final Map<String, String> config;
    private final ElasticsearchConnectionFactory factory;

    public DefaultElasticsearchCoordinateFactory(ElasticsearchConnectionFactory factory, Map<String, ?> config) {
        this.factory = factory;
        this.config = Maps.newHashMap();

        config.forEach((key, value) -> {
            if (value != null) {
                this.config.put(key, String.valueOf(value));
            }});
    }

    @Override
    public ElasticsearchCoordinate getElasticsearchCoordinate(NodeState indexDefinition) {
        return ElasticsearchCoordinateImpl.construct(factory, indexDefinition, config);
    }

    @Override
    public ElasticsearchIndexCoordinate getElasticsearchIndexCoordinate(IndexDefinition indexDefinition) {
        return null;
    }
}

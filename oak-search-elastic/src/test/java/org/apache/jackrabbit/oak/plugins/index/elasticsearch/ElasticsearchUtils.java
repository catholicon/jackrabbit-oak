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

import org.jetbrains.annotations.NotNull;

import java.net.HttpURLConnection;
import java.net.URL;

class ElasticsearchUtils {
    private static String createHealthURL(@NotNull final ElasticsearchCoordinate esCoords) {
        return esCoords.getScheme() + "://" + esCoords.getHost() + ":" + esCoords.getPort() + "/_cat/health";
    }

    static boolean isAvailable(@NotNull final ElasticsearchCoordinate esCoords) {
        try {
            URL url = new URL(createHealthURL(esCoords));

            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            int responseCode = con.getResponseCode();

            return responseCode == 200;
        } catch (Throwable t) {
            return false;
        }
    }
}

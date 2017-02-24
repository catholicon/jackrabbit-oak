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
package org.apache.jackrabbit.oak.plugins.index.lucene.indexStructure;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class DumpNodeState {
    private static String SINGLE_INDENT = "  ";

    static void logNode(NodeState node, String rootNodeName) {
        logNode(node, rootNodeName, "");
    }
    static void logNode(NodeState node, String rootNodeName, String indent) {
        log(String.format("%1$s+%2$s", indent, rootNodeName));
        for (PropertyState ps : node.getProperties()) {
            if (Utils.isHiddenPath("/" + ps.getName()))continue;
            if (JcrConstants.JCR_PRIMARYTYPE.equals(ps.getName()))continue;
            logProperty(ps, indent + SINGLE_INDENT);
        }
        for (ChildNodeEntry cne : node.getChildNodeEntries()) {
            if (Utils.isHiddenPath("/" + cne.getName()))continue;
            logNode(cne.getNodeState(), cne.getName(), indent + SINGLE_INDENT);
        }
    }

    private static void logProperty(PropertyState ps, String indent) {
        log(indent + "-" + ps.toString());
    }

    private static void log(String str) {
        System.out.println(str);
    }
}

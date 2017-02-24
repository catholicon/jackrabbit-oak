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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class IndexedFieldsTest extends AbstractQueryTest {
    private NodeStore nodeStore;

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        nodeStore = new MemoryNodeStore();
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    @Before
    public void setup() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.setProperty("foo", "fox jumping");
        test.addChild("testChild").setProperty("bar", "dog jumping");

        test.addChild("test1").addChild("testChild").setProperty("bar", "dog jumping");
        test.addChild("test2").addChild("testChild").setProperty("barX", "dog jumping");
        root.commit();

        NodeState rootState = nodeStore.getRoot();

        System.out.println("----------------CONTENT-------------------");
        DumpNodeState.logNode(rootState.getChildNode("test"), "/test");
    }

    @Test
    public void propertyIndexText() throws Exception {
        indexStructure("propIdx", new AugmentIndexDef() {
            @Override
            IndexDefinitionBuilder.PropertyRule augmentPropRule(IndexDefinitionBuilder.PropertyRule pr) {
                return pr.propertyIndex();
            }
        },
        "[foo]='test string'");
    }

    @Test
    public void analyzedIndexText() throws Exception {
        indexStructure("analyzedIdx", new AugmentIndexDef() {
            @Override
            IndexDefinitionBuilder.PropertyRule augmentPropRule(IndexDefinitionBuilder.PropertyRule pr) {
                return pr.analyzed();
            }
        },
        "contains([foo], 'test string')");
    }

    @Test
    public void nodeScopeIndexText() throws Exception {
        indexStructure("nodeScopedIdx", new AugmentIndexDef() {
            @Override
            IndexDefinitionBuilder.PropertyRule augmentPropRule(IndexDefinitionBuilder.PropertyRule pr) {
                return pr.nodeScopeIndex();
            }
        },
                "contains(*, 'test string')");
    }

    @Test
    public void evaluatePathRestrictions() throws Exception {
        indexStructure("evalPathRestriction",  new AugmentIndexDef() {
            @Override
            IndexDefinitionBuilder augmentIndexConfig(IndexDefinitionBuilder idb) {
                return idb.evaluatePathRestrictions();
            }
            @Override
            IndexDefinitionBuilder.PropertyRule augmentPropRule(IndexDefinitionBuilder.PropertyRule pr) {
                return pr.propertyIndex();
            }
        },
                "[foo]='test string' AND ISDESCENDANTNODE('/test/test1')");
    }
//
//    @Test
//    public void aggregate() throws Exception {
//        indexStructure("aggregate",  new AugmentIndexDef() {
//            @Override
//            void addIncludes(IndexDefinitionBuilder.AggregateRule aggRule) {
//                aggRule.include("*");
//            }
//        });
//    }

    private void indexStructure(String indexName, AugmentIndexDef aid, String cond) throws Exception {
        Tree oakIndex = root.getTree("/oak:index");
        createIndex(oakIndex, indexName,aid);
        dumpIndex(nodeStore.getRoot(), indexName);
        explainQuery(cond, indexName);
    }

    private void dumpIndex(NodeState root, String indexName) throws IOException {
        System.out.println("\n----------------" + indexName + "--------------");
        System.out.println("Definition");
        System.out.println("----------");
        DumpNodeState.logNode(root.getChildNode("oak:index").getChildNode(indexName), "/oak:index/" + indexName);
        System.out.println("Index");
        System.out.println("-----");
        LuceneIndexParser.getIndexStructure(root, indexName).dump();
    }

    private void explainQuery(String cond, String indexName) {
        System.out.println("-------------------------");
        String query = "SELECT * FROM [nt:base] WHERE " + cond;
        System.out.println("Query-> " + query);

        String explainOut = executeQuery("explain " + query, SQL2).get(0);
        String startPosMarker = indexName + ") ";
        int startIdx = explainOut.indexOf(startPosMarker) + startPosMarker.length();
        int endIdx = explainOut.indexOf(" ft:(");
        endIdx = (endIdx == -1)?explainOut.indexOf('\n'):endIdx;
        explainOut = explainOut.substring(startIdx, endIdx);

        System.out.println("LuceneQuery-> " + explainOut);
    }

    private String createIndex(Tree oakIndex, String indexName, AugmentIndexDef aid) throws CommitFailedException {
        Tree indexTree = oakIndex.addChild(indexName);
        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
        IndexDefinitionBuilder idb = aid.augmentIndexConfig(idxBuilder.includedPaths("/test"));
        IndexDefinitionBuilder.IndexRule rule = idb.indexRule("nt:base");
        aid.augmentPropRule(rule.property("foo"));
        aid.augmentPropRule(rule.property("bar", "testChild/bar"));
        aid.augmentPropRule(rule.property("allBar", "testChild/ba.*", true));
//        aid.addIncludes(idb.aggregateRule("nt:base"));

        idxBuilder.noAsync().build(indexTree);
        root.commit();
        return indexName;
    }

    class AugmentIndexDef {
        IndexDefinitionBuilder augmentIndexConfig(IndexDefinitionBuilder idb) {
            return idb;
        }
        IndexDefinitionBuilder.PropertyRule augmentPropRule(IndexDefinitionBuilder.PropertyRule pr) {
            return pr;
        }
//        void addIncludes(IndexDefinitionBuilder.AggregateRule aggRule) {
//        }
    }
}

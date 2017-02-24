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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LuceneIndexParser {
    public static IndexStructure getIndexStructure(NodeState root, String indexName) throws IOException {
        return getIndexStructure(root, indexName, "");
    }
    public static IndexStructure getIndexStructure(NodeState root, String indexName, String indent) throws IOException {
        NodeState idx = root.getChildNode("oak:index").getChildNode(indexName);

        OakDirectory dir = new OakDirectory(idx.builder(),
                new IndexDefinition(root, idx, "/oak:index/" + indexName),
                true);
        IndexStructure index = new IndexStructure();
        index.indent = indent;

        DirectoryReader reader = DirectoryReader.open(dir);
        for (AtomicReaderContext arc : reader.leaves()) {
            AtomicReader ar = arc.reader();
            Fields flds = ar.fields();

            Iterator<String> fldsIter = flds.iterator();
            while (fldsIter.hasNext()) {
                String fld = fldsIter.next();

                if (FieldNames.PATH.equals(fld)) continue;

                Bits matchAll = new Bits.MatchAllBits(ar.getDocCount(fld));

                Terms terms = flds.terms(fld);
                long size = terms.size();

                TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);
                for (long i = 0; i < size; i++) {
                    BytesRef termBR = termsEnum.next();
                    String term = termBR.utf8ToString();
                    if (FieldNames.PATH_DEPTH.equals(fld)) {
                        int depth = NumericUtils.prefixCodedToInt(termBR);
                        if (depth == 0) continue;
                        term = Integer.valueOf(depth).toString();
                    }

                    DocsEnum docsEnum = termsEnum.docs(matchAll, null);
                    int docId = docsEnum.nextDoc();
                    while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = ar.document(docId);
                        index.add(fld, term, doc.get(":path"));

                        docId = docsEnum.nextDoc();
                    }

                }
            }
        }

        return index;
    }

    static class IndexStructure {
        String indent = "";
        Map<String, FieldStructure> fields = new LinkedHashMap<>();
        void add(String field, String term, String path) {
            FieldStructure fld = fields.get(field);
            if (fld == null) {
                fld = new FieldStructure();
                fld.indent = indent + "  ";
                fields.put(field, fld);
            }
            fld.add(term, path);
        }

        void dump() {
            for (Map.Entry<String, FieldStructure> fieldEntry : fields.entrySet()) {
                System.out.println(indent + fieldEntry.getKey());
                fieldEntry.getValue().dump();
            }
        }
    }

    static class FieldStructure {
        String indent = "";
        Map<String, TermStructure> terms = new LinkedHashMap<>();
        void add(String term, String path) {
            TermStructure ter = terms.get(term);
            if (ter == null) {
                ter = new TermStructure();
                terms.put(term, ter);
            }
            ter.add(path);
        }

        void dump() {
            for (Map.Entry<String, TermStructure> termEntry : terms.entrySet()) {
                System.out.println(indent + termEntry.getKey() + " => " + termEntry.getValue().paths);
            }
        }
    }

    static class TermStructure {
        List<String> paths = new ArrayList<>();
        void add(String path) {
            paths.add(path);
        }
    }
}

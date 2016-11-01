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
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.base.Objects.equal;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.api.observation.JackrabbitObservationManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.observation.filter.FilterFactory;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.Selectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
// Don't run "Parallelized" as this causes tests to timeout in "weak" environments
public class ObservationTest extends AbstractRepositoryTest {
    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final String TEST_NODE = "test_node";
    private static final String REFERENCEABLE_NODE = "\"referenceable\"";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final String TEST_TYPE = "mix:test";
    public static final int TIME_OUT = 60;

    private Session observingSession;
    private ObservationManager observationManager;
    private String test_uuid;

    public ObservationTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();

        NodeTypeManager ntMgr = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName(TEST_TYPE);
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node n = session.getRootNode().addNode(TEST_NODE);
        n.setProperty("test_property1", 42);
        n.setProperty("test_property2", "forty_two");
        n.addMixin(TEST_TYPE);
        Node refNode = n.addNode(REFERENCEABLE_NODE);
        refNode.addMixin(JcrConstants.MIX_REFERENCEABLE);
        test_uuid = refNode.getProperty(JcrConstants.JCR_UUID).getString();

        session.save();

        observingSession = createAdminSession();
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        if (observingSession != null) {
            observingSession.logout();
        }
    }

    @Test
    public void observation() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.setProperty("p0", "v0"));
            Node n1 = listener.expectAdd(n.addNode("n1"));
            listener.expectAdd(n1.setProperty("p1", "v1"));
            listener.expectAdd(n1.setProperty("p2", "v2"));
            listener.expectAdd(n.addNode("n2"));
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.setProperty("property", 42));
            Node n3 = listener.expectAdd(n.addNode("n3"));
            listener.expectAdd(n3.setProperty("p3", "v3"));
            listener.expectChange(n1.setProperty("p1", "v1.1"));
            listener.expectRemove(n1.getProperty("p2")).remove();
            listener.expectRemove(n.getNode("n2")).remove();
            listener.expectAdd(n.addNode("{4}"));
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void infoMap() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        Node n3 = n.addNode("n3");
        n3.setProperty("p1", "q1");
        n3.setProperty("p2", "q2");
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            n.addNode("n1", "oak:Unstructured");
            n.addNode("n2");
            n.getNode("n2").addMixin(TEST_TYPE);
            n3.setProperty("p1", "changed");
            n3.setProperty("p2", (String) null);

            listener.expect(new Expectation("infoMap for n1") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    if (event.getType() == NODE_ADDED && event.getPath().endsWith("n1")) {
                        Map<?, ?> info = event.getInfo();
                        return info != null &&
                                "oak:Unstructured".equals(info.get(JCR_PRIMARYTYPE));
                    } else {
                        return false;
                    }
                }
            });
            listener.expect(new Expectation("infoMap for n1/jcr:primaryType") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    if (event.getType() == PROPERTY_ADDED &&
                            event.getPath().endsWith("n1/jcr:primaryType")) {
                        Map<?, ?> info = event.getInfo();
                        return info != null &&
                                "oak:Unstructured".equals(info.get(JCR_PRIMARYTYPE));
                    } else {
                        return false;
                    }
                }
            });

            listener.expect(new Expectation("infoMap for n2") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    if (event.getType() == NODE_ADDED && event.getPath().endsWith("n2")) {
                        Map<?, ?> info = event.getInfo();
                        if (info == null) {
                            return false;
                        }
                        Object mixinTypes = info.get(JCR_MIXINTYPES);
                        if (!(mixinTypes instanceof String[])) {
                            return false;
                        }

                        Object primaryType = info.get(JCR_PRIMARYTYPE);
                        String[] mixins = (String[]) mixinTypes;
                        return NT_UNSTRUCTURED.equals(primaryType) &&
                                mixins.length == 1 &&
                                TEST_TYPE.equals(mixins[0]);
                    } else {
                        return false;
                    }
                }
            });
            listener.expect(new Expectation("n2/jcr:primaryType") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return event.getType() == PROPERTY_ADDED &&
                            event.getPath().endsWith("n2/jcr:primaryType");
                }
            });
            listener.expect(new Expectation("n2/jcr:mixinTypes") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return event.getType() == PROPERTY_ADDED &&
                            event.getPath().endsWith("n2/jcr:mixinTypes");
                }
            });

            listener.expect(new Expectation("infoMap for n3/p1") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    if (event.getType() == PROPERTY_CHANGED &&
                            event.getPath().endsWith("n3/p1")) {
                        Map<?, ?> info = event.getInfo();
                        return info != null &&
                                NT_UNSTRUCTURED.equals(info.get(JCR_PRIMARYTYPE));
                    } else {
                        return false;
                    }
                }
            });
            listener.expect(new Expectation("infoMap for n3/p2") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    if (event.getType() == PROPERTY_REMOVED &&
                            event.getPath().endsWith("n3/p2")) {
                        Map<?, ?> info = event.getInfo();
                        return info != null &&
                                NT_UNSTRUCTURED.equals(info.get(JCR_PRIMARYTYPE));
                    } else {
                        return false;
                    }
                }
            });

            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observation2() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.addNode("n1"));
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.addNode("n2"));
            listener.expectRemove(n.getNode("n1")).remove();
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void typeFilter() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null,
                new String[]{TEST_TYPE}, false);

        try {
            Node n = getNode(TEST_PATH);
            Property p = n.setProperty("p", "v");
            listener.expectAdd(p);
            Node n1 = n.addNode("n1");
            listener.expect(n1.getPath(), NODE_ADDED);
            n1.addNode("n2");
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectChange(p).setValue("v2");
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectRemove(p).remove();
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void uuidFilter() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true,
                new String[]{test_uuid}, null, false);

        try {
            Node nonRefNode = getNode(TEST_PATH);
            Node refNode = nonRefNode.getNode(REFERENCEABLE_NODE);

            nonRefNode.addNode("n");
            listener.expect(refNode.addNode("r").getPath(), NODE_ADDED);
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void identifier() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expect(new Expectation("Has correct id") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return (TEST_PATH + "/newNode").equals(event.getIdentifier());
                }
            });

            n.addNode("newNode");
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observationOnRootNode() throws Exception {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, PROPERTY_ADDED, "/", true, null, null, false);
        try {
            // add property to root node
            Node root = getNode("/");
            listener.expectAdd(root.setProperty("prop", "value"));
            root.getSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void pathFilter() throws Exception {
        final String path = "/events/only/here";

        for (boolean deep : new boolean[]{false, true}) {
            Node root = getNode("/");
            if (root.hasNode("events")) {
                root.getNode("events").remove();
                root.getSession().save();
            }

            ExpectationListener listener = new ExpectationListener();
            observationManager.addEventListener(listener, NODE_ADDED, path, deep, null, null, false);
            try {
                root.addNode("events").addNode("only").addNode("here").addNode("below").addNode("this");
                listener.expect("/events/only/here/below", NODE_ADDED);
                if (deep) {
                    listener.expect("/events/only/here/below/this", NODE_ADDED);
                }
                root.getSession().save();

                List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
                assertTrue("Missing events: " + missing, missing.isEmpty());
                List<Event> unexpected = listener.getUnexpected();
                assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
            }
            finally {
                observationManager.removeEventListener(listener);
            }
        }
    }

    @Test
    public void pathFilterWithTrailingSlash() throws Exception {
        final String path = "/events/only/here";
        ExpectationListener listener = new ExpectationListener();
        listener.expect(new Expectation(path){
            @Override
            public boolean onEvent(Event event) throws Exception {
                return PathUtils.isAncestor(path, event.getPath());
            }
        });

        observationManager.addEventListener(listener, NODE_ADDED, path + '/', true, null, null, false);
        try {
            Node root = getNode("/");
            root.addNode("events").addNode("only").addNode("here").addNode("at");
            root.getSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observationDispose()
            throws RepositoryException, InterruptedException, ExecutionException, TimeoutException {

        final ExpectationListener listener = new ExpectationListener();
        Expectation hasEvents = listener.expect(
                new Expectation("has events after registering"));
        final Expectation noEvents = listener.expect(
                new Expectation("has no more events after unregistering", false));

        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        final Session s = getAdminSession();
        // Generate events
        ScheduledExecutorService service = newSingleThreadScheduledExecutor();
        service.scheduleWithFixedDelay(new Runnable() {
            private int c;

            @Override
            public void run() {
                try {
                    s.getNode(TEST_PATH)
                            .addNode("c" + c++)
                            .getSession()
                            .save();
                }
                catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10, 10, TimeUnit.MILLISECONDS);

        // Make sure we see the events
        assertNotNull(hasEvents.get(TIME_OUT, TimeUnit.SECONDS));

        // Remove event listener
        Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                observationManager.removeEventListener(listener);
                noEvents.enable(true);
                return null;
            }
        }).get(10, TimeUnit.SECONDS);

        // Make sure we see no more events
        assertFalse(noEvents.wait(4, TimeUnit.SECONDS));

        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void observationDisposeFromListener()
            throws RepositoryException, InterruptedException, ExecutionException, TimeoutException {

        final ExpectationListener listener = new ExpectationListener();
        Expectation unregistered = listener.expect(new Expectation
                ("Unregistering listener from event handler should not block") {
            @Override
            public boolean onEvent(Event event) throws Exception {
                observationManager.removeEventListener(listener);
                return true;
            }
        });

        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        // Ensure the listener is there
        assertTrue(observationManager.getRegisteredEventListeners().hasNext());

        // Generate events
        Node n = getNode(TEST_PATH);
        n.addNode("c");
        n.getSession().save();

        // Make sure we see the events and the listener is gone
        assertNotNull(unregistered.get(TIME_OUT, TimeUnit.SECONDS));
        assertFalse(observationManager.getRegisteredEventListeners().hasNext());
    }

    @Test
    public void testMove() throws RepositoryException, ExecutionException, InterruptedException {
        Node testNode = getNode(TEST_PATH);
        Session session = testNode.getSession();
        Node nodeA = testNode.addNode("a");
        Node nodeAA = nodeA.addNode("aa");
        Node nodeT = testNode.addNode("t");
        Node nodeS = testNode.addNode("s");
        session.save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_MOVED, "/", true, null, null, false);

        String src1 = nodeA.getPath();
        String dst1 = nodeT.getPath() + "/b";
        session.move(src1, dst1);
        listener.expectMove(src1, dst1);

        String src2 = nodeT.getPath() + "/b/aa";
        String dst2 = nodeS.getPath() + "/bb";
        session.move(src2, dst2);
        listener.expectMove(src1 + "/aa", dst2);

        session.save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void testRename() throws RepositoryException, ExecutionException, InterruptedException {
        Node testNode = getNode(TEST_PATH);
        Session session = testNode.getSession();
        Node nodeA = testNode.addNode("a");
        String parentPath = testNode.getPath();
        session.save();
        assumeTrue(nodeA instanceof JackrabbitNode);

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_MOVED, "/", true, null, null, false);

        ((JackrabbitNode) nodeA).rename("b");
        listener.expectMove(parentPath + "/a", parentPath + "/b");

        session.save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void testReorder() throws RepositoryException, InterruptedException, ExecutionException {
        Node testNode = getNode(TEST_PATH);
        Node nodeA = testNode.addNode("a", "nt:unstructured");
        Node nodeB = testNode.addNode("b", "nt:unstructured");
        testNode.getSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_MOVED, "/", true, null, null, false);
        listener.expect(new Expectation("orderBefore") {
            @Override
            public boolean onEvent(Event event) throws Exception {
                if (event.getType() != NODE_MOVED || event.getInfo() == null) {
                    return false;
                }

                Map<?, ?> info = event.getInfo();
                if (PathUtils.concat(TEST_PATH, "a").equals(event.getPath())) {
                    return "a".equals(info.get("srcChildRelPath")) &&
                            "b".equals(info.get("destChildRelPath"));
                } else if (PathUtils.concat(TEST_PATH, "b").equals(event.getPath())) {
                    return "b".equals(info.get("srcChildRelPath")) &&
                            "a".equals(info.get("destChildRelPath"));
                } else {
                    return false;
                }
            }
        });

        testNode.orderBefore(nodeA.getName(), null);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void addSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        Node n = getNode(TEST_PATH);
        Node a = listener.expectAdd(n.addNode("a"));
        Node b = listener.expectAdd(a.addNode("b"));
        listener.expectAdd(b.addNode("c"));
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void removeSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        n.addNode("a").addNode("b").addNode("c");
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        listener.expectRemove(n.getNode("a")).remove();
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void moveSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        n.addNode("a").addNode("b").addNode("c");
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        getAdminSession().move(TEST_PATH + "/a", TEST_PATH + "/t");
        listener.expect(TEST_PATH + "/t", NODE_MOVED);
        listener.expect(TEST_PATH + "/a", NODE_REMOVED);
        listener.expect(TEST_PATH + "/a/jcr:primaryType", PROPERTY_REMOVED);
        listener.expect(TEST_PATH + "/t", NODE_ADDED);
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterDisjunctPaths()
            throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof JackrabbitObservationManager);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();
        builder.condition(builder.any(
                builder.path(TEST_PATH + "/a/b"),
                builder.path(TEST_PATH + "/x/y")));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node b = testNode.addNode("a").addNode("b");
        b.addNode("c");
        Node y = testNode.addNode("x").addNode("y");
        y.addNode("z");

        listener.expect(b.getPath(), NODE_ADDED);
        listener.expect(y.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void disjunctPaths() throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof JackrabbitObservationManager);
        JackrabbitObservationManager oManager = (JackrabbitObservationManager) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter()
                .setAdditionalPaths(TEST_PATH + "/a", TEST_PATH + "/x")
                .setEventTypes(NODE_ADDED);
        oManager.addEventListener(listener, filter);

        Node testNode = getNode(TEST_PATH);
        Node b = testNode.addNode("a").addNode("b");
        b.addNode("c");
        Node y = testNode.addNode("x").addNode("y");
        y.addNode("z");

        listener.expect(b.getPath(), NODE_ADDED);
        listener.expect(y.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void noDuplicates() throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof JackrabbitObservationManager);
        JackrabbitObservationManager oManager = (JackrabbitObservationManager) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter()
                .setAdditionalPaths(TEST_PATH + "/a", TEST_PATH + "/a")
                .setEventTypes(NODE_ADDED);
        oManager.addEventListener(listener, filter);

        Node testNode = getNode(TEST_PATH);
        Node b = testNode.addNode("a").addNode("b");
        b.addNode("c");
        Node y = testNode.addNode("x").addNode("y");
        y.addNode("z");

        listener.expect(b.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void pathExclude() throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof JackrabbitObservationManager);
        JackrabbitObservationManager oManager = (JackrabbitObservationManager) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter()
                .setAbsPath(TEST_PATH)
                .setIsDeep(true)
                .setExcludedPaths(TEST_PATH + "/c", TEST_PATH + "/d",  "/x/y")
                .setEventTypes(ALL_EVENTS);
        oManager.addEventListener(listener, filter);

        Node n = getNode(TEST_PATH);
        listener.expectAdd(listener.expectAdd(
                listener.expectAdd(n.addNode("a")).addNode("a1")).setProperty("p", "q"));
        listener.expectAdd(
                listener.expectAdd(n.addNode("b")).setProperty("p", "q"));
        n.addNode("c").addNode("c1").setProperty("p", "q");
        n.addNode("d").setProperty("p", "q");
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void parentPathExclude() throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof JackrabbitObservationManager);

        Node n = getNode(TEST_PATH).addNode("n");
        getAdminSession().save();

        JackrabbitObservationManager oManager = (JackrabbitObservationManager) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter()
                .setAbsPath(n.getPath())
                .setIsDeep(true)
                .setExcludedPaths(n.getParent().getPath())
                .setEventTypes(ALL_EVENTS);
        oManager.addEventListener(listener, filter);

        n.addNode("n1");
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterPropertyOfParent()
            throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Events for all items whose parent has a property named "foo" with value "bar"
        builder.condition(builder.property(Selectors.PARENT, "foo",
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState property) {
                        return "bar".equals(property.getValue(STRING));
                    }
                }));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = testNode.addNode("a");
        Node x = testNode.addNode("x");
        a.setProperty("foo", "bar");
        x.setProperty("foo", "baz");
        a.addNode("b");
        x.addNode("y");

        listener.expect(a.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
        listener.expect(a.getPath() + "/foo", PROPERTY_ADDED);
        listener.expect(a.getPath() + "/b", NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterPropertyOfChild()
            throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Events for all items that have a property "b/c/foo" with value "bar"
        builder.condition(builder.property(Selectors.fromThis("b/c"), "foo",
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState property) {
                        return "bar".equals(property.getValue(STRING));
                    }
                }));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = testNode.addNode("a");
        a.addNode("b").addNode("c").setProperty("foo", "bar");
        a.addNode("d");
        Node x = testNode.addNode("x");
        x.addNode("b").addNode("c").setProperty("foo", "baz");
        x.addNode("d");

        listener.expect(a.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void addSubtreeFilter() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Only generate events for the root of added sub trees
        builder.condition(builder.addSubtree());
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = listener.expectAdd(testNode.addNode("a"));
        a.addNode("c");
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void removeSubtreeFilter() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a").addNode("c");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Only generate events for the root of deleted sub trees
        builder.condition(builder.deleteSubtree());
        oManager.addEventListener(listener, builder.build());

        listener.expectRemove(testNode.getNode("a")).remove();
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void propertyValue() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            n.setProperty("added", 42);
            listener.expectValue(null, n.getProperty("added").getValue());

            Value before = n.getProperty("test_property1").getValue();
            n.getProperty("test_property1").setValue(43);
            listener.expectValue(before, n.getProperty("test_property1").getValue());

            before = n.getProperty("test_property2").getValue();
            n.getProperty("test_property2").remove();
            listener.expectValue(before, null);
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void propertyValues() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        n.setProperty("toChange", new String[]{"one", "two"}, PropertyType.STRING);
        n.setProperty("toDelete", new String[]{"three", "four"}, PropertyType.STRING);
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            n.setProperty("added", new String[]{"five", "six"}, PropertyType.STRING);
            listener.expectValues(null, n.getProperty("added").getValues());

            Value[] before = n.getProperty("toChange").getValues();
            n.getProperty("toChange").setValue(new String[]{"1", "2"});
            listener.expectValues(before, n.getProperty("toChange").getValues());

            before = n.getProperty("toDelete").getValues();
            n.getProperty("toDelete").remove();
            listener.expectValues(before, null);
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }

    //------------------------------------------------------------< ExpectationListener >---

    private static class Expectation extends ForwardingListenableFuture<Event> {
        private final SettableFuture<Event> future = SettableFuture.create();
        private final String name;

        private volatile boolean enabled = true;

        Expectation(String name, boolean enabled) {
            this.name = name;
            this.enabled = enabled;
        }

        Expectation(String name) {
            this(name, true);
        }

        @Override
        protected ListenableFuture<Event> delegate() {
            return future;
        }

        public void enable(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void complete(Event event) {
            future.set(event);
        }

        public boolean isComplete() {
            return future.isDone();
        }

        public void fail(Exception e) {
            future.setException(e);
        }

        public boolean wait(long timeout, TimeUnit unit) {
            try {
                future.get(timeout, unit);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }

        public boolean onEvent(Event event) throws Exception {
            return true;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class ExpectationListener implements EventListener {
        private final Set<Expectation> expected = synchronizedSet(
                Sets.<Expectation>newCopyOnWriteArraySet());
        private final List<Event> unexpected = synchronizedList(
                Lists.<Event>newCopyOnWriteArrayList());

        private volatile Exception failed;

        public Expectation expect(Expectation expectation) {
            if (failed != null) {
                expectation.fail(failed);
            }
            expected.add(expectation);
            return expectation;
        }

        public Future<Event> expect(final String path, final int type) {
            return expect(new Expectation("path = " + path + ", type = " + type) {
                @Override
                public boolean onEvent(Event event) throws RepositoryException {
                    return type == event.getType() && equal(path, event.getPath());
                }
            });
        }

        public Future<Event> expect(final String path, final String identifier, final int type) {
            return expect(new Expectation("path = " + path + ", identifier = " + identifier + ", type = " + type) {
                @Override
                public boolean onEvent(Event event) throws RepositoryException {
                    return type == event.getType() && equal(path, event.getPath()) && equal(identifier, event.getIdentifier());
                }
            });
        }

        public Node expectAdd(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_ADDED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
            return node;
        }

        public Node expectRemove(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_REMOVED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_REMOVED);
            return node;
        }

        public Property expectAdd(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_ADDED);
            return property;
        }

        public Property expectRemove(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_REMOVED);
            return property;
        }

        public Property expectChange(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_CHANGED);
            return property;
        }

        public void expectMove(final String src, final String dst) {
            expect(new Expectation('>' + src + ':' + dst){
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return event.getType() == NODE_MOVED &&
                            equal(dst, event.getPath()) &&
                            equal(src, event.getInfo().get("srcAbsPath")) &&
                            equal(dst, event.getInfo().get("destAbsPath"));
                }
            });
        }

        public void expectValue(final Value before, final Value after) {
            expect(new Expectation("Before value " + before + " after value " + after) {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return equal(before, event.getInfo().get("beforeValue")) &&
                           equal(after, event.getInfo().get("afterValue"));
                }
            });
        }

        public void expectValues(final Value[] before, final Value[] after) {
            expect(new Expectation("Before valuse " + before + " after values " + after) {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return Arrays.equals(before, (Object[])event.getInfo().get("beforeValue")) &&
                           Arrays.equals(after, (Object[]) event.getInfo().get("afterValue"));
                }
            });
        }

        public Future<Event> expectBeforeValue(final String path, final int type, final String beforeValue) {
            return expect(new Expectation("path = " + path + ", type = " + type + ", beforeValue = " + beforeValue) {
                @Override
                public boolean onEvent(Event event) throws RepositoryException {
                    return type == event.getType() && equal(path, event.getPath()) && event.getInfo().containsKey("beforeValue") && beforeValue.equals(((Value)event.getInfo().get("beforeValue")).getString());
                }
            });
        }

        public List<Expectation> getMissing(int time, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException {
            List<Expectation> missing = Lists.newArrayList();
            long t0 = System.nanoTime();
            try {
                Futures.allAsList(expected).get(time, timeUnit);
            }
            catch (TimeoutException e) {
                for (Expectation exp : expected) {
                    if (!exp.isDone()) {
                        missing.add(exp);
                    }
                }
            }
            return missing;
        }

        public List<Event> getUnexpected() {
            return Lists.newArrayList(unexpected);
        }

        @Override
        public void onEvent(EventIterator events) {
            try {
                while (events.hasNext() && failed == null) {
                    Event event = events.nextEvent();
                    boolean found = false;
                    for (Expectation exp : expected) {
                        if (exp.isEnabled() && !exp.isComplete() && exp.onEvent(event)) {
                            found = true;
                            exp.complete(event);
                        }
                    }
                    if (!found) {
                        unexpected.add(event);
                    }

                }
            } catch (Exception e) {
                for (Expectation exp : expected) {
                    exp.fail(e);
                }
                failed = e;
            }
        }

        private static String key(String path, int type) {
            return path + ':' + type;
        }
    }
    
    //------------------------------------------------------------< OakEventFilter tests >---

    @Test
    public void applyNodeTypeOnSelf() throws Exception {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a", "nt:unstructured").addNode("b", "oak:Unstructured").addNode("c", "nt:unstructured");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter.setAbsPath("/");
        filter.setIsDeep(true);
        filter.setNodeTypes(new String[] {"oak:Unstructured"});
        filter = FilterFactory.wrap(filter).withApplyNodeTypeOnSelf();

        oManager.addEventListener(listener, filter);

        testNode.getNode("a").getNode("b").getNode("c").remove();
        testNode.getSession().save();

        // wait 1 sec to give failures a chance (we're not expecting anything, but perhaps
        // something would come, after 1sec more likely than after 0sec)
        Thread.sleep(1000);
        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

        testNode.getNode("a").getNode("b").remove();
        testNode.getSession().save();

        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    
    }

    @Test
    public void includeAncestorsRemove() throws Exception {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a").addNode("b").addNode("c").addNode("d").setProperty("e", 42);
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter.setAbsPath(TEST_PATH + "/a/b/c/d");
        filter.setIsDeep(true);
        filter = FilterFactory.wrap(filter).withIncludeAncestorsRemove();

        oManager.addEventListener(listener, filter);

        Node d = testNode.getNode("a").getNode("b").getNode("c").getNode("d");
        Property e = d.getProperty("e");
        listener.expectRemove(e);
//        listener.expectRemove(d.getProperty("jcr:primaryType"));
//        d.remove();
        listener.expectRemove(d).remove();
        testNode.getSession().save();

        Thread.sleep(1000);
        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());

        oManager.addEventListener(new EventListener() {
            
            @Override
            public void onEvent(EventIterator events) {
                while(events.hasNext()) {
                    System.out.println("GOT: "+events.next());
                }
                
            }
        }, NODE_REMOVED, TEST_PATH + "/a", false, null, null, false);
        System.out.println("REGISTERED");
        
        testNode = getNode(TEST_PATH);
        Node b = testNode.getNode("a").getNode("b");
        listener.expect(b.getPath(), NODE_REMOVED);
        b.remove();
        // but not the jcr:primaryType
        testNode.getSession().save();

        Thread.sleep(1000);
        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        unexpected = listener.getUnexpected();
        assertTrue("Missing events: " + missing, missing.isEmpty());
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    
        Node a = testNode.getNode("a");
        listener.expect(a.getPath(), NODE_REMOVED);
        a.remove();
        // but not the jcr:primaryType
        testNode.getSession().save();

        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());
    }

    @Test
    public void includeRemovedSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a").addNode("c");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter.setAbsPath("/");
        filter.setIsDeep(true);
        filter = FilterFactory.wrap(filter).withIncludeSubtreeOnRemove();

        oManager.addEventListener(listener, filter);

        listener.expectRemove(testNode.getNode("a").getNode("c"));
        listener.expectRemove(testNode.getNode("a")).remove();
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }
    
    @Test
    public void includeRemovedSubtree_BeforeValue() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        Node a = testNode.addNode("a");
        a.setProperty("propA", "24");
        a.addNode("c").setProperty("propB", "42");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter.setAbsPath("/");
        filter.setIsDeep(true);
        filter = FilterFactory.wrap(filter).withIncludeSubtreeOnRemove();

        oManager.addEventListener(listener, filter);

        Node c = testNode.getNode("a").getNode("c");
        listener.expectRemove(c);
        listener.expectBeforeValue(c.getProperty("propB").getPath(), PROPERTY_REMOVED, "42");
        a = testNode.getNode("a");
        listener.expectBeforeValue(a.getProperty("propA").getPath(), PROPERTY_REMOVED, "24");
        listener.expectRemove(a).remove();
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }
    
    @Test
    public void includeGlobPaths() throws Exception {
        
        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a1").addNode("b").addNode("c");
        testNode.addNode("a2").addNode("b").addNode("c");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter = FilterFactory.wrap(filter).withIncludeGlobPaths(TEST_PATH + "/a2/**");

        oManager.addEventListener(listener, filter);

        testNode.getNode("a1").getNode("b").remove();
        listener.expectRemove(testNode.getNode("a2").getNode("b")).remove();
        testNode.getSession().save();
        
        Thread.sleep(1000);
        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        
        Node a3 = testNode.addNode("a3");
        Node foo = a3.addNode("bar").addNode("foo");
        testNode.getSession().save();

        filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
//        filter.setAbsPath(TEST_PATH + "/a3/bar/foo/x");
        filter = FilterFactory.wrap(filter).withIncludeGlobPaths(TEST_PATH + "/a3/**/x");
        oManager.addEventListener(listener, filter);
        
        Node x = foo.addNode("x");
        listener.expect(x.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
        testNode.getSession().save();

        Thread.sleep(1000);
        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }
    
    @Test
    public void testConsecutiveGlobPaths() throws Exception {
        Node testNode = getNode(TEST_PATH);
        Node a1 = testNode.addNode("a1");
        a1.addNode("b1").addNode("c1");
        a1.addNode("b2").addNode("c2");
        testNode.addNode("a2").addNode("b").addNode("c");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(ALL_EVENTS);
        filter = FilterFactory.wrap(filter).withIncludeGlobPaths(TEST_PATH + "/a2/**").withIncludeGlobPaths(TEST_PATH + "/a1/**");

        oManager.addEventListener(listener, filter);

        listener.expectRemove(testNode.getNode("a1").getNode("b2")).remove();
        listener.expectRemove(testNode.getNode("a2").getNode("b")).remove();
        testNode.getSession().save();
        
        Thread.sleep(1000);
        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void testAggregate1() throws Exception {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setAbsPath("/parent");
        filter.setIsDeep(true);
        filter.setEventTypes(ALL_EVENTS);
        filter = FilterFactory.wrap(filter).withNodeTypeAggregate(new String[] { "oak:Unstructured" },
                new String[] { "", "jcr:content", "jcr:content/**" });
        oManager.addEventListener(listener, filter);
        Node parent = getAdminSession().getRootNode().addNode("parent", "nt:unstructured");
        Node child = parent.addNode("child", "nt:unstructured");
        Node file = child.addNode("file", "oak:Unstructured");
        listener.expectAdd(file);
        Node jcrContent = file.addNode("jcr:content", "nt:unstructured");
        listener.expect(jcrContent.getPath(), "/parent/child/file", NODE_ADDED);
        listener.expect(jcrContent.getPath() + "/jcr:primaryType", "/parent/child/file", PROPERTY_ADDED);
        Property jcrDataProperty = jcrContent.setProperty("jcr:data", "foo");
        listener.expect(jcrDataProperty.getPath(), "/parent/child/file", PROPERTY_ADDED);
        parent.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());

        file = getAdminSession().getRootNode().getNode("parent").getNode("child").getNode("file");
        jcrContent = file.getNode("jcr:content");
        Property newProperty = jcrContent.setProperty("newProperty", "foo");
        listener.expect(newProperty.getPath(), "/parent/child/file", PROPERTY_ADDED);
        Property lastModifiedBy = jcrContent.setProperty("jcr:lastModifiedBy", "bar");
        listener.expect(lastModifiedBy.getPath(), "/parent/child/file", PROPERTY_ADDED);
        jcrContent.getSession().save();

        Thread.sleep(2000);
        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());
    }

    @Test
    public void testAggregate2() throws Exception {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setAbsPath("/parent");
        filter.setIsDeep(true);
        filter.setEventTypes(ALL_EVENTS);
        filter = FilterFactory.wrap(filter).withNodeTypeAggregate(new String[] { "oak:Unstructured" },
                new String[] { "", "**" });// "file", "file/jcr:content",
                                           // "file/jcr:content/**");
        oManager.addEventListener(listener, filter);
        Node parent = getAdminSession().getRootNode().addNode("parent", "nt:unstructured");
        Node child = parent.addNode("child", "oak:Unstructured");
        listener.expectAdd(child);
        Node file = child.addNode("file", "nt:unstructured");
        listener.expectAdd(file);
        Node jcrContent = file.addNode("jcr:content", "nt:unstructured");
        listener.expect(jcrContent.getPath(), "/parent/child", NODE_ADDED);
        listener.expect(jcrContent.getPath() + "/jcr:primaryType", "/parent/child", PROPERTY_ADDED);
        Property jcrDataProperty = jcrContent.setProperty("jcr:data", "foo");
        listener.expect(jcrDataProperty.getPath(), "/parent/child", PROPERTY_ADDED);
        parent.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());

        file = getAdminSession().getRootNode().getNode("parent").getNode("child").getNode("file");
        jcrContent = file.getNode("jcr:content");
        Property newProperty = jcrContent.setProperty("newProperty", "foo");
        listener.expect(newProperty.getPath(), "/parent/child", PROPERTY_ADDED);
        Property lastModifiedBy = jcrContent.setProperty("jcr:lastModifiedBy", "bar");
        listener.expect(lastModifiedBy.getPath(), "/parent/child", PROPERTY_ADDED);
        jcrContent.getSession().save();

        Thread.sleep(2000);
        missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());
    }

    @Test
    public void testAggregate3() throws Exception {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setAbsPath("/parent");
        filter.setIsDeep(true);
        filter.setEventTypes(ALL_EVENTS);
        filter = FilterFactory.wrap(filter).withNodeTypeAggregate(new String[] { "oak:Unstructured" },
                new String[] { "**" } );
        oManager.addEventListener(listener, filter);
        
        Node parent = getAdminSession().getRootNode().addNode("parent", "nt:unstructured");
        Node child = parent.addNode("child", "nt:unstructured");
//        listener.expectAdd(child);
        Node file = child.addNode("file", "oak:Unstructured");
        listener.expect(file.getPath(), "/parent/child/file", NODE_ADDED);
        listener.expect(file.getPath() + "/jcr:primaryType", "/parent/child/file", PROPERTY_ADDED);
        Node jcrContent = file.addNode("jcr:content", "nt:unstructured");
        listener.expect(jcrContent.getPath(), "/parent/child/file", NODE_ADDED);
        listener.expect(jcrContent.getPath() + "/jcr:primaryType", "/parent/child/file", PROPERTY_ADDED);
        Property jcrDataProperty = jcrContent.setProperty("jcr:data", "foo");
        listener.expect(jcrDataProperty.getPath(), "/parent/child/file", PROPERTY_ADDED);
        parent.getSession().save();

        Thread.sleep(1000);
        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        assertTrue("Missing events: " + missing, missing.isEmpty());
    }
    
}

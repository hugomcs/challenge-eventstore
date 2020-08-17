package net.intelie.challenges;

import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InMemoryEventStoreTest {

    @Test
    public void testInsertSuccess() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 3L));
        store.insert(new Event("a", 1L));
        store.insert(new Event("a", 2L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 2L));
        store.insert(new Event("b", 10L));

        Map<String, List<Event>> events = store.getEvents();
        // Testing if the lists are ordered correctly
        assertEquals(1L, events.get("a").get(0).timestamp());
        assertEquals(2L, events.get("a").get(1).timestamp());
        assertEquals(2L, events.get("a").get(2).timestamp());
        assertEquals(3L, events.get("a").get(3).timestamp());
        assertEquals(4L, events.get("a").get(4).timestamp());
        assertEquals(10L, events.get("b").get(0).timestamp());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertNegativeTimestamp() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", -1L));
    }

    @Test
    public void testRemoveAllSuccess() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 3L));
        store.insert(new Event("a", 1L));
        store.removeAll("a");
        assertEquals(0, store.getEvents().size());
    }

    @Test
    public void testQuerySuccessCase1() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 3L));
        store.insert(new Event("a", 1L));
        store.insert(new Event("a", 2L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 2L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query("a", 1L, 4L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("1;2;2;3;", sb.toString());
    }

    @Test
    public void testQuerySuccessCase2() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 1L));
        store.insert(new Event("a", 5L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 7L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 3L));
        store.insert(new Event("a", 9L));
        store.insert(new Event("a", 8L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 15L));
        store.insert(new Event("a", 13L));
        store.insert(new Event("a", 70L));
        store.insert(new Event("a", 20L));
        store.insert(new Event("a", 17L));
        store.insert(new Event("a", 8L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query("a", 3L, 9L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("3;4;4;4;5;7;8;8;", sb.toString());
    }

    @Test
    public void testQuerySuccessCase3() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 1L));
        store.insert(new Event("a", 5L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 7L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 3L));
        store.insert(new Event("a", 9L));
        store.insert(new Event("a", 8L));
        store.insert(new Event("a", 4L));
        store.insert(new Event("a", 15L));
        store.insert(new Event("a", 13L));
        store.insert(new Event("a", 70L));
        store.insert(new Event("a", 20L));
        store.insert(new Event("a", 17L));
        store.insert(new Event("a", 8L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query("a", 1L, 5L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("1;3;4;4;4;", sb.toString());
    }

    @Test
    public void testQuerySuccessCase4() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 1L));
        store.insert(new Event("a", 5L));
        store.insert(new Event("a", 4L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query("a", -100L, 100L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("1;4;5;", sb.toString());
    }

    @Test
    public void testQuerySuccessCase5() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event(null, 1L));
        store.insert(new Event(null, 4L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query(null, 1L, 3L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("1;", sb.toString());
    }

    @Test
    public void testQuerySuccessCase6() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.insert(new Event("a", 4L));

        StringBuilder sb = new StringBuilder();
        try(EventIterator it = store.query("a", 5L, 6L)) {
            while(it.moveNext()) {
                sb.append(it.current().timestamp()).append(';');
            }
        }
        assertEquals("", sb.toString());
    }

    @Test
    public void testThreadSafe() throws InterruptedException {
        final InMemoryEventStore store = new InMemoryEventStore();
        for(long i = 0; i < 100000; i++) {
            store.insert(new Event("a", i));
        }
        final StringBuilder trace = new StringBuilder();

        final Thread writer = new Thread(() -> {
            for(long i = 0; i < 50000; i++) {
                store.insert(new Event("a", i));
                assertEquals(store.getEvents().get("a").size(), i+1);
            }
            trace.append(5);
        });

        final Thread remover = new Thread(() -> {
            try(EventIterator it = store.query("a", 0L, 100001)) {
                trace.append(3);
                writer.start();
                int count = 0;
                while(it.moveNext()) {
                    it.remove();
                    count++;
                }
                trace.append(4);
                assertEquals(100000, count);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });

        Thread reader = new Thread(() -> {
            try(EventIterator it = store.query("a", 0L, 100001)) {
                trace.append(1);
                remover.start();
                Set<Long> elements = new HashSet<>();
                while(it.moveNext()) {
                    if(!elements.add(it.current().timestamp())) {
                        fail("The same element was read twice");
                    }
                }
                trace.append(2);
                assertEquals(100000, elements.size());
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });
        reader.start();

        reader.join();
        remover.join();
        writer.join();

        assertEquals("12345", trace.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryInvalidRangeCase1() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.query("a", 2L, 1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryInvalidRangeCase2() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.query("a", 2L, 2L);
    }
}

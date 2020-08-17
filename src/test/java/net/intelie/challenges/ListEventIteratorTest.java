package net.intelie.challenges;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class ListEventIteratorTest {

    @Test
    public void testMoveNextAndCurrentSuccess() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 3L));
        list.add(new Event("a", 1L));
        list.add(new Event("a", 2L));
        list.add(new Event("a", 4L));
        list.add(new Event("a", 2L));
        list.add(new Event("b", 10L));

        try (ListEventIterator it = new ListEventIterator(list, 1, 5, null)) {
            assertTrue(it.moveNext());
            assertEquals(it.current().timestamp(), 1L);
            assertTrue(it.moveNext());
            assertEquals(it.current().timestamp(), 2L);
            assertTrue(it.moveNext());
            assertEquals(it.current().timestamp(), 4L);
            assertTrue(it.moveNext());
            assertEquals(it.current().timestamp(), 2L);
            assertFalse(it.moveNext());
        }
    }

    @Test
    public void testCloseSuccess() throws Exception {
        final Lock lock = new ReentrantLock();
        lock.lock();

        try (ListEventIterator it = new ListEventIterator(new ArrayList<>(), 0, 1, lock)) {
            Thread lockChecker = new Thread(() -> {
                assertFalse(lock.tryLock());
            });
            lockChecker.start();
            lockChecker.join();
        }

        Thread lockChecker = new Thread(() -> {
            assertTrue(lock.tryLock());
        });
        lockChecker.start();
        lockChecker.join();
    }

    @Test(expected = IllegalStateException.class)
    public void testMoveNextAndCurrentInvalidState() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 3L));
        try (ListEventIterator it = new ListEventIterator(list, 0, 1, null)) {
            it.moveNext();
            it.moveNext();
            it.current();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCurrentNotInitialized() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 3L));
        try (ListEventIterator it = new ListEventIterator(list, 1, 5, null)) {
            it.current();
        }
    }

    @Test
    public void testMoveNextAndRemoveSuccess() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 1L));
        list.add(new Event("a", 2L));
        list.add(new Event("a", 3L));

        try (ListEventIterator it = new ListEventIterator(list, 0, 3, null)) {
            it.moveNext();
            it.moveNext();
            it.remove();
            assertEquals(1L, it.current().timestamp());
            it.moveNext();
            assertEquals(3L, it.current().timestamp());
            assertFalse(it.moveNext());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testRemoveNotInitialized() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 3L));
        try (ListEventIterator it = new ListEventIterator(list, 1, 5, null)) {
            it.remove();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testMoveNextAndRemoveInvalidState() throws Exception {
        List<Event> list = new ArrayList<>();
        list.add(new Event("a", 1L));

        try (ListEventIterator it = new ListEventIterator(list, 0, 1, null)) {
            it.moveNext();
            it.moveNext();
            it.remove();
        }
    }

}

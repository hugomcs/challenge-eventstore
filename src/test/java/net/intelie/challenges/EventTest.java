package net.intelie.challenges;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventTest {

    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void testCompareToDifferentCase1() {
        Event oneEvent = new Event("a", 1L);
        Event otherEvent = new Event("a", 2L);
        assertTrue(oneEvent.compareTo(otherEvent) < 0);
    }

    @Test
    public void testCompareToDifferentCase2() {
        Event oneEvent = new Event("a", 2L);
        Event otherEvent = new Event("a", 1L);
        assertTrue(oneEvent.compareTo(otherEvent) > 0);
    }

    @Test
    public void testCompareToEquals() {
        Event oneEvent = new Event("a", 1L);
        Event otherEvent = new Event("a", 1L);
        assertTrue(oneEvent.compareTo(otherEvent) != 0);
    }
}
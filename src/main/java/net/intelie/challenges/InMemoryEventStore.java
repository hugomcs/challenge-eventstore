package net.intelie.challenges;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory implementation of the {@link EventStore}.
 *
 * <p><br>This implementation stores the events on an {@link ArrayList} for each event type.
 * This collection has been chosen to optimize the memory usage and the performance of the
 * method {@link #query(String, long, long)}, so it can run in O(log(n)) time complexity, performing a binary search.
 *
 * <p><br>The main costs of this approach are the performance of both {@link #insert(Event)} and
 * {@link EventIterator#remove()} operations. Since the list must always be sorted,
 * both runs in O(n) time, because they must shift all the remaining lesser/greater
 * elements, when an element is added/removed on the middle of the list.
 *
 * <p><br>The lists are stored in a {@link HashMap}, so they can be accessed in near O(1) time by it's type.
 *
 * <p><br>This implementation is Thread-safe.
 *
 * <p><br>NOTE: On a real application, we would possibly be able to assume that each inserted event is
 * always more recent than the others, and then generate the timestamp inside the server. This would
 * reduce the complexity of the {@link #insert(Event)} to a simple append. Since this is not specified,
 * the implementation doesn't assume that, and has to do an ordered insert considering the received timestamp.
 *
 * <p><br>NOTE: Other simple alternative to the {@link ArrayList} approach would be to use a {@link TreeSet}.
 * This approach would allow {@link #insert(Event)} and {@link EventIterator#remove()} operations to run
 * in O(log(n)) time complexity, but the {@link #query(String, long, long)} would not be as fast. It would
 * also consume much more memory. I've considered that it's more important to have the best performance
 * on the {@link #query(String, long, long)} method because it would impact directly on the user experience
 * on a real application, and the insert and remove methods could be called in an asynchronous way, without
 * compromising the application performance.
 *
 * @author Hugo Saldanha (saldanha.hugo@gmail.com)
 */
public class InMemoryEventStore implements EventStore {

    /** Holds a List of events for each event type */
    private final Map<String, List<Event>> events = new HashMap<>();

    /** Holds a lock for each event type */
    private final Map<String, Lock> locks = new HashMap<>();

    /**
     * Stores an event in O(n) time complexity.
     * @param event The event to be inserted.
     */
    @Override
    public void insert(Event event) {
        if(event.timestamp() < 0) {
            throw new IllegalArgumentException("Event timestamp must be non-negative");
        }
        Lock lock;
        // Since we don't have the lock obj yet, we have to synchronize to "this" just to get/create the lock
        synchronized(this) {
            lock = locks.computeIfAbsent(event.type(), k -> new ReentrantLock());
        }
        lock.lock();
        List<Event> list = events.computeIfAbsent(event.type(), k -> new ArrayList<>());
        try {
            int idx = -(Collections.binarySearch(list, event))-1; // This runs in O(log(n))
            list.add(idx, event); // This runs in O(n) because of the shifts to the right
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes all events of specific type in near O(1) time complexity.
     * @param type The type of the events to be removed.
     */
    @Override
    public void removeAll(String type) {
        events.remove(type);
        locks.remove(type);
    }

    /**
     * Retrieves an iterator for events based on their type and timestamp in O(log(n)) time complexity.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return The iterator of events contained in the time range
     */
    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        if(startTime >= endTime) {
            throw new IllegalArgumentException("endTime must be greater than startTime");
        }
        List<Event> list = events.get(type);
        Lock lock = locks.get(type);

        lock.lock();
        Range r = rangeBinarySearch(list, startTime, endTime);

        return new ListEventIterator(list, r.low, r.high, lock);
    }



    /**
     * Performs a binary search on the event list returning the range of indexes
     * containing the two given timestamps.
     *
     * <p><br>The low index in the range returned will be
     * the lowest index containing an element equals to <i>timestamp1</i> (to consider repetitions),
     * or the highest index containing an element lower than <i>timestamp1</i>,
     * if it's not found.
     *
     * <p><br>The high index in the range returned will be
     * the highest index containing an element equals to <i>timestamp2</i> (to consider repetitions),
     * or the lowest index containing an element higher than <i>timestamp2</i>,
     * if it's not found.
     *
     * <p><br>This method runs in O(log(n)) time complexity, where n is the size of the list.
     *
     * <p><br>NOTE: I chose to implement my own binary search to optimize the second search of the highest
     * timestamp. On the first search, I'm comparing both timestamp1 and 2 to the current element,
     * and reducing the range needed to be searched on the second search.
     *
     * <p><br>NOTE: Implementing as non-recursive to optimize memory usage.
     *
     * @param list The event list to be searched
     * @param timestamp1 the lowest timestamp to search
     * @param timestamp2 the highest timestamp to search
     *
     * @return the range of indexes containing the two given timestamps.
     */
    private static Range rangeBinarySearch(List<Event> list, long timestamp1, long timestamp2) {
        int nextLow = 0;
        int low = nextLow;
        int nextHigh = list.size()-1;
        int high = nextHigh;

        Range range = new Range();

        while (low <= high) {

            /* Using the unsigned shift to avoid overflows,
             * since java doesn't support unsigned int values
             */
            int mid = (low + high) >>> 1;

            long midVal = list.get(mid).timestamp();

            if(timestamp1 <= midVal) {
                high = mid - 1;
                if(timestamp2 <= midVal) {
                    // Optimizing the high index of the second search
                    nextHigh = high;
                }
            } else {
                low = mid + 1;
                if(timestamp2 > midVal) {
                    // Optimizing the low index of the second search
                    nextLow = low;
                }
            }
            /* I'm not checking if I found the exact element, so the algorithm works
             * with repeated timestamps on the list, as it should.
             */

        }
        nextLow = Math.max(nextLow, low);
        range.low = low;

        /* I'm doing the second loop here instead of in a common method because the loops
         * are slightly different, and creating a common method to be called twice would
         * become more confusing to read and understand.
         */
        while (nextLow <= nextHigh) {
            int mid = (nextLow + nextHigh) >>> 1;
            long midVal = list.get(mid).timestamp();

            if(timestamp2 <= midVal) {
                nextHigh = mid - 1;
            } else {
                nextLow = mid + 1;
            }
        }
        range.high = nextLow;

        return range;
    }

    /** Package visibility getter for test purposes */
    Map<String, List<Event>> getEvents() {
        return events;
    }

    /** Basic structure to hold a range of positions in the list */
    private static class Range {
        int low;
        int high;
    }

}

package net.intelie.challenges;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * An iterator over an event list.
 *
 * <p><br>This iterator performs best with a {@link java.util.RandomAccess} List.
 *
 * <p><br>This implementation is package-visible because it's designed to be used
 * by the {@link InMemoryEventStore}, and is Thread-safe only if an already locked lock
 * is provided. The lock will be released when the iterator is closed.
 *
 * @author Hugo Saldanha (saldanha.hugo@gmail.com)
 */
class ListEventIterator implements EventIterator {

    private List<Event> list;

    private final int startIdx;
    private int endIdx;
    private int cursor = -1;

    private Lock lock;

    /**
     * Creates a new {@link ListEventIterator}, that iterates over the provided range
     * of the given list.
     *
     * @param list The list
     * @param startIdx The end index, inclusive
     * @param endIdx The end index, exclusive
     * @param lock Already locked lock to be released on close
     */
    ListEventIterator(List<Event> list, int startIdx, int endIdx, Lock lock) {
        this.list = list;
        this.startIdx = Math.max(startIdx, 0);
        this.endIdx = Math.min(endIdx, list.size());
        this.lock = lock;
    }

    /**
     * Move the iterator to the next event, if any.
     *
     * @return false if the iterator has reached the end, true otherwise.
     */
    @Override
    public boolean moveNext() {
        if(cursor == -1) {
            cursor = startIdx;
        } else {
            cursor++;
        }
        return cursor < endIdx;
    }

    /**
     * Gets the current event ref'd by this iterator.
     * This runs in O(1) time complexity only if the
     * list is a {@link java.util.RandomAccess} list
     *
     * @return the event itself.
     * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
     */
    @Override
    public Event current() {
        if(cursor == -1 || cursor == endIdx) {
            throw new IllegalStateException("Invalid cursor position.");
        }
        return list.get(cursor);
    }

    /**
     * Remove current event from its store.
     * This runs in O(n) if the list is a RandomAccess list
     * since all the greater elements must be shifted to the left.
     *
     * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
     */
    @Override
    public void remove() {
        if(cursor == -1 || cursor == endIdx) {
            throw new IllegalStateException("Invalid cursor position.");
        }
        list.remove(cursor);
        cursor--;
        endIdx--;
    }

    /**
     * Releases the lock and all the other resources used by this iterator.
     */
    @Override
    public void close() {
        if(lock != null) {
            lock.unlock();
            lock = null;
        }
        list = null;
    }
}

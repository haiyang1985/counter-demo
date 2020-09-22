package org.ghy.counter;

public interface CounterService {
    /**
     * Get current value from counter
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     */
    void get(final boolean readOnlySafe, final CounterClosure closure);

    /**
     * Add delta to counter then get value
     */
    void incrementAndGet(final long delta, final CounterClosure closure);
}

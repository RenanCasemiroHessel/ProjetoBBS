package com.projetobbs;

import java.util.concurrent.atomic.AtomicLong;

public class LogicalClock {
    private final AtomicLong counter = new AtomicLong(0);

    public long tick() {
        return counter.incrementAndGet();
    }

    public long update(long received) {
        long updated = Math.max(counter.get(), received) + 1;
        counter.set(updated);
        return updated;
    }

    public long get() {
        return counter.get();
    }
}
package test;

import org.apache.ignite.events.CacheEvent;

public interface CacheUpdateListener {
    void onUpdate(CacheEvent event);
}

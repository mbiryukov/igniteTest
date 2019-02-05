package test;

import org.apache.ignite.events.CacheEvent;

public class CacheUpdateListenerImpl implements CacheUpdateListener {
    @Override
    public void onUpdate(CacheEvent event) {
        System.out.println("newValue=" + event.newValue());
    }
}

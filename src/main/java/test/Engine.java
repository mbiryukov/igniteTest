package test;

import org.apache.ignite.*;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

@Component
public class Engine {
    private static final String CACHE_NAME = "testCache";
    private Ignite ignite;

    public Engine() {
    }

    @PostConstruct
    public void init() {
        if (Ignition.state() == IgniteState.STOPPED) {
            //ignite = Ignition.start();
            ignite = Ignition.start("ignite-config.xml");
        } else if (Ignition.state() == IgniteState.STARTED) {
            ignite = Ignition.ignite();
        }
    }

    public void put(Integer key, Data value) {
        IgniteCache cache = ignite.getOrCreateCache(CACHE_NAME);
        try {
            cache.put(key, value);
        } catch (TransactionException e) {
            System.err.println(e);
        }
    }

    public Data get(Integer key) {
        IgniteCache cache = ignite.getOrCreateCache(CACHE_NAME);
        return (Data) cache.get(key);
    }

    public void addOrReplaceCollectionValue(Integer cacheKey, Integer key, String value) throws PutCacheException {
        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            Data data = get(cacheKey);
            data.addOrReplaceValue(key, value);
            Data data1 = get(cacheKey);
            put(cacheKey, data);
            tx.commit();
        } catch (IgniteException e) {
            e.printStackTrace();
            throw new PutCacheException();
        }
        System.out.println("finished thread");
    }

    public void setLocalListenerAnyKey(CacheUpdateListener cacheUpdateListener) {
        IgnitePredicate<CacheEvent> localListener = createListenerAnyKey(cacheUpdateListener);
        ignite.events(ignite.cluster().forCacheNodes("smartws")).localListen(localListener, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
    }

    public void setRemoteListenerAnyKey(CacheUpdateListener cacheUpdateListener) {
        IgnitePredicate<CacheEvent> remoteListener = createListenerAnyKey(cacheUpdateListener);
        ignite.events(ignite.cluster().forCacheNodes("smartws")).remoteListenAsync(null, remoteListener, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
    }

    public void setLocalListenerCurrentKey(CacheUpdateListener cacheUpdateListener, Integer key) {
        IgnitePredicate<CacheEvent> localListener = createListenerCurrentKey(key, cacheUpdateListener);
        ignite.events(ignite.cluster().forCacheNodes("smartws")).localListen(localListener, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
    }

    public void setRemoteListenerCurrentKey(CacheUpdateListener cacheUpdateListener, Integer key) {
        IgnitePredicate<CacheEvent> remoteListener = createListenerCurrentKey(key, cacheUpdateListener);
        ignite.events(ignite.cluster().forCacheNodes("smartws")).remoteListenAsync(null, remoteListener, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
    }

    private IgnitePredicate<CacheEvent> createListenerAnyKey(final CacheUpdateListener cacheUpdateListener) {
        return cacheEvent -> {
            cacheUpdateListener.onUpdate(cacheEvent);
            return true;
        };
    }

    private IgnitePredicate<CacheEvent> createListenerCurrentKey(Integer listenedKey, final CacheUpdateListener cacheUpdateListener) {
        return cacheEvent -> {
            cacheUpdateListener.onUpdate(cacheEvent);
            return cacheEvent.key().equals(listenedKey);
        };
    }
}

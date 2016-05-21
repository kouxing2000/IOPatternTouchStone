package com.shark.iopattern.touchstone.server.xinglang;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * A concurrent map implementation with order support. The map rely on
 * an scheduler to remove expired node periodically. 
 * 
 * @author wanxingl
 * 
 * @param <K>
 *            Key class
 * @param <V>
 *            Value class
 */
public final class NewFastMap<K, V> implements java.util.Map<K, V> {

    // Back end map.
    private final ConcurrentHashMap<K, KeyValuePair<SortKey<K>, V>> innerMap;
    private final ConcurrentSkipListSet<SortKey<K>> sortList;
   
    private final static class SortKey<K> implements Comparable <SortKey<K>>{
        private final long timestamp;
        private final K key;
        
        SortKey(K key) {
            this.timestamp = System.currentTimeMillis();
            this.key = key;
        }
        
        SortKey(long timestamp) {
            this.timestamp = timestamp;
            this.key = null;
        }
        
        @SuppressWarnings("unchecked")
        public boolean equals(Object obj) {
            SortKey<K> other = (SortKey<K>) obj;
            if (other.timestamp != this.timestamp) {
                return false;
            }
            if (key != null) {
                return key.equals(other.key);
            } else {
                return key == other.key;
            }
        }

        @Override
        public int compareTo(SortKey<K> o) {
            return (int)(this.timestamp - o.timestamp);

        }
        
        @Override
        public String toString() {
            return "[key=" + key + ",timestamp=" + timestamp + "]";
        }
    }
    
    // Value wrapper class.
    private final static class KeyValuePair<K, V> {
        private K key;
        
        // Should use volatile to guarantee the visibility
        private volatile V value;

        KeyValuePair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "[key=" + key + ",value=" + value + "]";
        }
    }

    /**
     * Construct a fast map.
     */
    public NewFastMap() {
        innerMap = new ConcurrentHashMap<K, KeyValuePair<SortKey<K>, V>>();
        sortList = new ConcurrentSkipListSet<SortKey<K>>();
    }

    @Override
    public void clear() {
        sortList.clear();
        innerMap.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        KeyValuePair<SortKey<K>, V> entry;
        if ((entry = innerMap.get(key)) != null) {
            return entry.value;
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return innerMap.keySet();
    }

    public List<V> getExpired(long timestamp) {
        SortKey<K> sortKey = new SortKey<K>(timestamp);
        List<V> expired = new LinkedList<V>();
        NavigableSet<SortKey<K>> headSet = sortList.headSet(sortKey, false);
        for (SortKey<K> key : headSet) {
            V v;
            if ((v = get(key.key)) != null) {
                expired.add(v);
            }
        }
        return expired;
    }
    
    public List<V> removeExpired(long timestamp) {
        SortKey<K> sortKey = new SortKey<K>(timestamp);
        List<V> expired = new LinkedList<V>();
        NavigableSet<SortKey<K>> headSet = sortList.headSet(sortKey, false);
        for (SortKey<K> key : headSet) {
            V v;
            if ((v = remove(key.key)) != null) {
                expired.add(v);
            }
        }
        return expired;
    }
    
    public long getEarliestTime() {
        try {
            return sortList.first().timestamp;
        } catch (NoSuchElementException ex) {
            return System.currentTimeMillis() + 60 * 1000;
        }
    }
    
    @Override
    public V put(K key, V value) {
        KeyValuePair<SortKey<K>, V> entry;
        SortKey<K> newKey = new SortKey<K>(key);
        synchronized (key) {
            if ((entry = innerMap.get(key)) != null) {
                V oldValue = entry.value;
                entry.value = value;
                sortList.remove(entry.key);
                sortList.add(newKey);
                return oldValue;
            } else {
                entry = new KeyValuePair<SortKey<K>, V>(newKey, value);
                sortList.add(newKey);
                innerMap.put(key, entry);
                return null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Iterator<?> iter = m.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry e = (Map.Entry) iter.next();
            put((K) e.getKey(), (V) e.getValue());
        }
    }

    @Override
    public V remove(Object key) {
        // Delegate it to inner map
        KeyValuePair<SortKey<K>, V> entry;
        synchronized (key) {
            if ((entry = innerMap.remove(key)) == null) {
                return null;
            }
            
            sortList.remove(entry.key);
            V v = entry.value;
            entry.value = null;
            return v;
        }
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("\nMap:");
        builder.append(innerMap);
        builder.append("\nKeyList:");
        builder.append(sortList);
        return builder.toString();
    }
}

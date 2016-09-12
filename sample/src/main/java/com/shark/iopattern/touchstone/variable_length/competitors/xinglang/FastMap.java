package com.shark.iopattern.touchstone.variable_length.competitors.xinglang;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * A concurrent map implementation with order support. The map rely on
 * an scheduler to remove expired node periodically. 
 * 
 * @author wanxingl
 * 
 * @param <K>
 *            Key class, must have hashcode
 * @param <V>
 *            Value class
 */
public final class FastMap<K, V> implements java.util.Map<K, V> {

    // Back end map.
    private final ConcurrentHashMap<K, FastMapEntry<K, V>> innerMap;
    // Queues for ordering
    private final Queue<K, V>[] segments;
    // Concurrency level of the queues
    private final int concurrencyLevel;
    private long nextTimestamp = Long.MAX_VALUE;

    // Copy from concurrent Hash Map
    private final int segmentShift;
    private final int segmentMask;
    private static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    // A queue to keep the order of key of ConcurrentHashMap
    private final static class Queue<K, V> {
        private final Node<K> head;
        // Tail of the queue, guard by head.
        private Node<K> tail;
        private Node<K> cacheHead;
        
        // Size of the node cache
        private AtomicInteger cachedSize = new AtomicInteger(0);
        // Length of the queue
        private AtomicInteger count = new AtomicInteger(0);
        
        private long lastExpiredTime = 0;
        // A temp node instance used to collect expired node.
        private final Node<K> expiredHead;
        private final Map<K, FastMapEntry<K, V>> innerMap;
        private final FastMap<K, V> fastMap;

        Queue(Map<K, FastMapEntry<K, V>> innerMap, FastMap<K, V> fastMap) {
            tail = head = new Node<K>(null);
            expiredHead = new Node<K>(null);
            this.innerMap = innerMap;
            this.fastMap = fastMap;
        }

        public Node<K> replace(K k, Node<K> n) {
            synchronized (head) {
                if (n.timestamp > lastExpiredTime) {
                    //Safe to replace
                    n.timestamp = System.currentTimeMillis();
                    if (n == tail) {
                        return n;
                    }
                    n.previous.next = n.next;
                    n.next.previous = n.previous;
                    n.previous = tail;
                    n.next = null;
                    return tail = tail.next = n;
                }
            }
            n.key = null; // the expired thread will cycle it. 
            count.decrementAndGet();
            return append(k);
        }
        
        // Append an node to the end of queue and return it.
        public Node<K> append(K k) {
            Node<K> t;
            synchronized (expiredHead) {
                if ((t = cacheHead) != null) {
                    cacheHead = t.next;
                }
            }
            if (t != null) {
                cachedSize.decrementAndGet();
                t.next = null;
                t.reset(k);
            } else {
                t = new Node<K>(k);
            }
            count.incrementAndGet();
            synchronized (head) {
                t.previous = tail;
                return tail = tail.next = t;
            }
        }

        // Remove and return expired values.
        public long removeExpired(long expiredTime, List<V> expiredValues, boolean removeFlag) {
            Node<K> previous = head;
            Node<K> current;
            Node<K> end;
            Node<K> expired = expiredHead;
            // Use synchronized to guarantee to get the latest tail node.
            synchronized (head) {
                lastExpiredTime = expiredTime;
                current = head.next;
                end = this.tail;
            }
            if (previous == end) {
                // No node in the queue
                return Long.MAX_VALUE;
            }
            previous = end;
            // Determine the end.
            while (current != end) {
                if (current.timestamp <= expiredTime) {
                    previous = current = current.next;
                } else {
                    break;
                }
            }
            
            end = previous;
            previous = head;
            current = head.next;
            
            int expiredCount = 0;
            int removedCount = 0;
            while (current != end) {
                Node<K> t = current;
                K key = current.key;
                if (key == null) {
                    // The key was accessed by get/put/remove, just compact it.
                    previous.next = current = current.next;
                    current.previous = previous;
                    t.next = null;
                    t.previous = null;
                    expired = expired.next = t;
                    expiredCount ++;
                } else if (expiredTime >= current.timestamp) {
                    if (removeFlag) {
                        // The key was expired.
                        previous.next = current = current.next;
                        current.previous = previous;
                        if (remove(t, expiredValues)) {
                            removedCount ++;
                        }
                        t.next = null;
                        t.previous = null;
                        expired = expired.next = t;
                        expiredCount ++;
                    } else {
                        previous = current;
                        current = current.next;
                        addExpiredValue(expiredValues, key);
                    }
                } else {
                    break;
                }
            }

            // Check the tail node expired or not. but keep the tail node even it expired.
            if (current == end && expiredTime >= current.timestamp) {
                if (removeFlag) {
                    if (remove(current, expiredValues)) {
                        removedCount ++;
                    }
                } else {
                    K key = current.key;
                    if (key != null) {
                        addExpiredValue(expiredValues, key);
                    }
                }
            }

            // Put expired nodes to cache and update the count.
            if (expiredHead.next != null) {
                synchronized (expiredHead) {
                    expired.next = cacheHead;
                    cacheHead = expiredHead.next;
                }
                expiredHead.next = null;
                cachedSize.addAndGet(expiredCount);
            }
            if (removedCount > 0) {
                count.addAndGet(-removedCount);
            }

            // Get the timestamp of the head node
            Node<K> next = head.next;
            if (next != null && next.key != null) {
                return next.timestamp;
            } else {
                return Long.MAX_VALUE;
            }
        }

        private void addExpiredValue(List<V> expiredValues, K key) {
            FastMapEntry<K, V> entry = innerMap.get(key);
            if (entry != null) {
                synchronized (entry) {
                    V v = entry.value;
                    if (v != null) {
                        expiredValues.add(v);
                    }
                }
            }
        }

        private void recycle(Node<K> node) {
            count.decrementAndGet();
            synchronized (head) {
                if (node.timestamp > lastExpiredTime) {
                    if (node != tail) {
                        node.previous.next = node.next;
                        node.next.previous = node.previous;
                    } else {
                        tail = node.previous;
                        tail.next = null;
                    }
                } else {
                    // the remove expired thread will cycle this.
                    return;
                }
            }
            node.previous = null;
            synchronized (expiredHead) {
                node.next = cacheHead;
                cacheHead = node;
            }
            cachedSize.incrementAndGet();
        }
        
        // Remove expired node.
        private boolean remove(Node<K> node, List<V> expired) {
            // Double check the node was accessed by other thread or not.
            // If it was accessed by other thread, the key will be null.
            K key = node.key;
            if (key == null) {
                return false;
            }
            FastMapEntry<K, V> oldEntry = innerMap.remove(key);
            if (oldEntry != null) {
                synchronized (oldEntry)
                {
                    if (oldEntry.node == node) {
                        // If the node is same, it is safe to add it to expired.
                        expired.add(oldEntry.value);
                        oldEntry.value = null;
                        oldEntry.node = null;
                        node.key = null;
                    } else {
                        // the value have been modified during compact. put it back
                        fastMap.put(key, oldEntry.value);
                        oldEntry.node.key = null;
                        oldEntry.node = null;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            Node<K> current;
            Node<K> end;
            // Get the latest tail node.
            synchronized (head) {
                current = head.next;
                end = this.tail;
            }

            StringBuilder builder = new StringBuilder();
            builder.append("\nQueue:");
            builder.append("CacheSize:" + cachedSize + ",");
            builder.append("Size:" + count + ",");
            while (current != null && current != end) {
                builder.append(current);
                builder.append(",");
                current = current.next;
            }
            if (head != end) {
                builder.append(end);
            }
            return builder.toString();
        }
    }

    // Value wrapper class.
    private final static class FastMapEntry<K, V> {
        // Should use volatile to guarantee the visibility
        private Node<K> node;
        
        // Value of the entry
        private V value;

        FastMapEntry(Node<K> node, V value) {
            this.node = node;
            this.value = value;
        }

        @Override
        public String toString() {
            return "[node=" + node + ",value=" + value + "]";
        }
    }

    // Node class of the queue
    private final static class Node<K> {
        // Timestamp of the node creation.
        long timestamp;
        // Should use volatile to guarantee the visibility
        volatile K key;
        Node<K> next;
        Node<K> previous;

        Node() {
        }
        
        Node(K key) {
            reset(key);
        }
        
        void reset(K key) {
            this.key = key;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "[timestamp=" + timestamp + ", key=" + key + "]";
        }
    }

    /**
     * Construct a fast map.
     */
    public FastMap() {
        this(0);
    }

    /**
     * Construct a fast map.
     * 
     * @param initialCapacity
     *            Initial capacity of the back end map
     */
    @SuppressWarnings("unchecked")
    public FastMap(int initialCapacity) {
        this.concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
        if (initialCapacity > 0) {
            innerMap = new ConcurrentHashMap<K, FastMapEntry<K, V>>(initialCapacity);
        } else {
            innerMap = new ConcurrentHashMap<K, FastMapEntry<K, V>>();
        }

        // Copy from Concurrent Hash map
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        segments = new Queue[ssize];
        
        int nodeCacheSize = Math.max(1, initialCapacity/concurrencyLevel);
        // Create queue and initialize the cache.
        for (int i = 0; i < ssize; i++) {
            segments[i] = new Queue<K, V>(innerMap, this);
            Node<K> currentCacheNode = new Node<K>();
            segments[i].cacheHead = currentCacheNode;
            segments[i].cachedSize.set(nodeCacheSize + 1);
            for (int j = 0, t = nodeCacheSize; j < t; j++) {
                currentCacheNode = currentCacheNode.next = new Node<K>();
            }
        }
    }

    @Override
    public void clear() {
        innerMap.clear();
        // it is not necessary to clear the queues. 
        // when node expired, the innerMap remove will return null.
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

//    @SuppressWarnings("unchecked")
    @Override
    public final V get(Object key) {
        // Just delegate get to the inner map
        FastMapEntry<K, V> entry = innerMap.get(key);
        if (entry != null) {
            synchronized (entry) {
                return entry.value;
            }
//          Queue<K, V> segment = segmentFor((K) key);
//          synchronized (entry) {
//              Node<K> node = entry.node;
//              if (node != null) {
//                  //if the node is null, the key has been removed. the return value is null.
//                  entry.node = segment.replace((K) key, node);
//              }
//              return entry.value;
//          }
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

    @Override
    public V put(K key, V value) {
        FastMapEntry<K, V> entry;
        if ((entry = innerMap.get(key)) != null) {
            synchronized (entry) {
                // If the entry existed, only replace the value and append a new node
                Node<K> currentNode;
                if ((currentNode = entry.node) == null) {
                    //The key has been removed. Should put it again.
                    return put(key, value);
                }
                entry.node = segmentFor(key).replace(key, currentNode);
                V v = entry.value;
                entry.value = value;
                return v;
            }
        } else {
            // If the entry not existed, create a new entry
            Node<K> node = segmentFor(key).append(key);
            if ((entry = innerMap.put(key, new FastMapEntry<K, V>(node, value))) != null) {
                synchronized (entry) {
                    // Another thread create prior current thread. Null the replaced node.
                    Node<K> currentNode = entry.node;
                    currentNode.key = null;
                    entry.node = null;
                    V v = entry.value;
                    entry.value = value;
                    segmentFor(key).recycle(currentNode);
                    return v;
                }
            }
            return null;
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

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        // Delegate it to inner map
        FastMapEntry<K, V> entry;
        if ((entry = innerMap.remove(key)) == null) {
            return null;
        }
        Queue<K, V> segment = segmentFor((K) key);
        synchronized (entry) {
            Node<K> currentNode = entry.node;
            currentNode.key = null;
            entry.node = null;
            V v = entry.value;
            // Should null the value here to avoid an thread get the removed value during the remove.
            entry.value = null;
            segment.recycle(currentNode);
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

    // Copy from concurrent hash map
    private static int hash(int h) {
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private final Queue<K, V> segmentFor(K k) {
        return segments[(hash(k.hashCode()) >>> segmentShift) & segmentMask];
    }

    public synchronized long getNextExpiredTime() {
        return nextTimestamp;
    }

    /**
     * Return expired values and remove it from the map.
     * 
     * @param expiredTime
     *            Expired time
     * @return List of expired values.
     */
    public synchronized List<V> removeExpired(long expiredTime) {
        List<V> expired = new LinkedList<V>();
        nextTimestamp = Long.MAX_VALUE;
        for (Queue<K, V> q : segments) {
            nextTimestamp = Math.min(q.removeExpired(expiredTime, expired, true), nextTimestamp);
        }
        return expired;
    }

    /**
     * Return expired values.
     * 
     * @param expiredTime
     *            Expired time
     * @return List of expired values.
     */
    public synchronized List<V> getExpired(long expiredTime) {
        List<V> expired = new LinkedList<V>();
        nextTimestamp = Long.MAX_VALUE;
        for (Queue<K, V> q : segments) {
            nextTimestamp = Math.min(q.removeExpired(expiredTime, expired, false), nextTimestamp);
        }
        return expired;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Next time stamp:" + nextTimestamp);
        builder.append("\nMap:");
        builder.append(innerMap);
        for (Queue<K, V> q : segments) {
            builder.append(q);
        }
        return builder.toString();
    }
    

    public static void main(String[] args) throws Exception {
        int count = 500000;
        int size = 50000;
        pftest(count, size, new FastMap<Integer, Integer>());
        pftest(count, size, new NewFastMap<Integer, Integer>());
        pftest(count, size, new ConcurrentHashMap<Integer, Integer>());
        pftest(count, size, new ConcurrentLinkedHashMap.Builder<Integer, Integer>().maximumWeightedCapacity(Integer.MAX_VALUE).build());
    }
    
    private static void pftest(final int count, final int size, final Map<Integer, Integer> m) {
        int threadNum = Runtime.getRuntime().availableProcessors();
        final java.util.concurrent.CountDownLatch latch = 
            new java.util.concurrent.CountDownLatch(threadNum);
        for (int t=0; t<threadNum; t++) {
            final int bb = t;
            new Thread() {
                @Override
                public void run() {
                    long begin = System.currentTimeMillis();
                    for (int i = (bb* count); i < (bb+1) * count; i++) {
                        Integer v;
                        v = m.put(i, i);
                        assert v == null || v.equals(i);
                        v = m.get(i);
                        assert v == null || v.equals(i);
                        v = m.get(i-size);
                        assert v == null || v.equals(i - size);
                        v = m.remove(i-size);
                        assert v == null || v.equals(i - size);
                    }
                    System.out.println("" + m.getClass() + ":" + (System.currentTimeMillis() - begin));
                    latch.countDown();
                }
            }.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
        System.out.println(m.size());
//        if (m instanceof FastMap) {
//            ((FastMap) m).removeExpired(System.currentTimeMillis());
//            System.out.println(m);
//        }
    }
}


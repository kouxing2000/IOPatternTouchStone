package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DisruptorDemo {

    class LongEvent {
        private long value;

        public void set(long value) {
            this.value = value;
        }
    }

    class LongEventFactory implements EventFactory<LongEvent> {
        public LongEvent newInstance() {
            return new LongEvent();
        }
    }

    class LongEventHandler implements WorkHandler<LongEvent> {
        public void onEvent(LongEvent event) {
            System.out.println(Thread.currentThread() + " Event: " + event.value);

//            try {
//                Thread.sleep(600);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    class LongEventProducer {
        private final RingBuffer<LongEvent> ringBuffer;

        public LongEventProducer(RingBuffer<LongEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void onData(ByteBuffer bb) {
            long sequence = ringBuffer.next();  // Grab the next sequence
            try {
                LongEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
                // for the sequence
                event.set(bb.getLong(0));  // Fill with data
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new DisruptorDemo().main();
    }

    void main() throws Exception {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor);

        // Connect the handler
        disruptor.handleEventsWithWorkerPool(new LongEventHandler(), new LongEventHandler());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        LongEventProducer producer = new LongEventProducer(ringBuffer);

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; l < 100; l++)
        {
            bb.putLong(0, l);
            producer.onData(bb);
            //Thread.sleep(200);
        }

        System.out.println("producer done!");
    }

}
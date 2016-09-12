package com.shark.iopattern.touchstone.variable_length;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;

public class VLServerSPI2 implements ServerSPI {

    Random random = new Random();

    Comparator<Integer> asc = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o2 - o1;
        }
    };

    Comparator<Integer> desc = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2;
        }
    };

    @Override
    public void process(ByteBuffer in, ByteBuffer out) {
        out.put(in);
        out.flip();

        // System.err.println(BytesHexDumper.getHexdump(out));

        int num = ServerConstants.SERVER_LOGIC_COMPLEXITY;

        List<Integer> ints = new ArrayList<Integer>();

        for (int i = 0; i < num; i++) {
            ints.add(i);
        }

        int randomValue = random.nextInt(num);
        int result = Collections.binarySearch(ints, randomValue);

        if (result < 0) {
            System.out.println("Error!");
        }

        Collections.sort(ints, desc);

        if (ServerConstants.SERVER_LOGIC_SLEEP_TIME > 0 && randomValue == 0) {
            try {
                Thread.sleep(ServerConstants.SERVER_LOGIC_SLEEP_TIME);
            } catch (InterruptedException e) {
            }
        }

        Collections.sort(ints, asc);

    }
}
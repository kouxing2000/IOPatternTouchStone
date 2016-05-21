package com.shark.iopattern.touchstone.share;

import java.nio.ByteBuffer;

/**
 * Provides utility methods for an Bytes
 * 
 */
public class BytesHexDumper {

	private static final byte[] highDigits;

	private static final byte[] lowDigits;

	// initialize lookup tables
	static {
		final byte[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
				'9', 'A', 'B', 'C', 'D', 'E', 'F' };

		int i;
		byte[] high = new byte[256];
		byte[] low = new byte[256];

		for (i = 0; i < 256; i++) {
			high[i] = digits[i >>> 4];
			low[i] = digits[i & 0x0F];
		}

		highDigits = high;
		lowDigits = low;
	}

	public static String getHexdump(ByteBuffer in) {
		return getHexdump(in, 16);
	}

	public static String getHexdump(ByteBuffer in, int lineLimit) {
		return getHexdump(in, lineLimit, Integer.MAX_VALUE);
	}

	public static String getHexdump(ByteBuffer in, int lineLimit,
			int lengthLimit) {
		if (lengthLimit == 0) {
			throw new IllegalArgumentException("lengthLimit: " + lengthLimit
					+ " (expected: 1+)");
		}

		boolean truncate = in.remaining() > lengthLimit;
		int size;
		if (truncate) {
			size = lengthLimit;
		} else {
			size = in.remaining();
		}

		if (size == 0) {
			return "empty";
		}

		StringBuffer out = new StringBuffer(size * 3 - 1);

		int mark = in.position();

		// fill the first
		int byteValue = in.get() & 0xFF;
		out.append((char) highDigits[byteValue]);
		out.append((char) lowDigits[byteValue]);
		size--;

		int no = 1;
		// and the others, too
		for (; size > 0; size--) {
			if (no % lineLimit == 0)
				out.append("\n");
			else
				out.append(' ');
			byteValue = in.get() & 0xFF;
			no++;
			out.append((char) highDigits[byteValue]);
			out.append((char) lowDigits[byteValue]);
		}

		in.position(mark);

		if (truncate) {
			out.append("...");
		}

		return out.toString();
	}

	public static String getHexdump(byte[] bytes) {
		return getHexdump(bytes, 16);
	}

	public static String getHexdump(byte[] bytes, int lineLimit) {
		return getHexdump(bytes, lineLimit, 0);
	}

	public static String getHexdump(byte[] bytes, int lineLimit, int startIndex) {
		return getHexdump(bytes, lineLimit, startIndex, Integer.MAX_VALUE);
	}

	public static String getHexdump(byte[] bytes, int lineLimit,
			int startIndex, int lengthLimit) {
		if (lengthLimit == 0) {
			throw new IllegalArgumentException("lengthLimit: " + lengthLimit
					+ " (expected: 1+)");
		}
		if (lineLimit == 0) {
			throw new IllegalArgumentException("lineLimit: " + lineLimit
					+ " (expected: 1+)");
		}

		boolean truncate = (bytes.length - startIndex) > lengthLimit;
		int size;
		if (truncate) {
			size = lengthLimit;
		} else {
			size = bytes.length - startIndex;
		}

		if (size == 0) {
			return "empty";
		}

		StringBuffer out = new StringBuffer(size * 3 - 1);

		int index = startIndex;

		// fill the first
		int byteValue = bytes[index++] & 0xFF;
		out.append((char) highDigits[byteValue]);
		out.append((char) lowDigits[byteValue]);
		size--;

		// and the others, too
		for (; size > 0; size--) {
			if ((index - startIndex) % lineLimit == 0)
				out.append("\n");
			else
				out.append(' ');
			byteValue = bytes[index++] & 0xFF;
			out.append((char) highDigits[byteValue]);
			out.append((char) lowDigits[byteValue]);
		}

		if (truncate) {
			out.append("...");
		}

		return out.toString();
	}
	
	public static ByteBuffer getByteBufferFromHexdump(String dumpContent) {
		return ByteBuffer.wrap(getBytesFromHexdump(dumpContent));
	}
	
	public static byte[] getBytesFromHexdump(String dumpContent) {
		String content = dumpContent.replace(" ", "").replace("\n", "");

		byte[] bytes = new byte[content.length() / 2];
		for (int i = 0; i < content.length();) {
			byte b1 = Byte.parseByte(content.substring(i, i + 1), 16);
			byte b2 = Byte.parseByte(content.substring(i + 1, i + 2), 16);
			byte b = (byte) (b1 * 16 + b2);
			bytes[i / 2] = b;
			i = i + 2;
		}
		return bytes;
	}

}
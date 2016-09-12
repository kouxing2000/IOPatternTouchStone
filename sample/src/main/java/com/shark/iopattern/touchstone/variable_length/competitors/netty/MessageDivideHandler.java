package com.shark.iopattern.touchstone.variable_length.competitors.netty;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.shark.iopattern.touchstone.server.ServerConstants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.shark.iopattern.touchstone.share.ProtocolSPI;


@Sharable
public class MessageDivideHandler extends FrameDecoder {
	
	private ProtocolSPI protocolSPI;
	
	public MessageDivideHandler(ProtocolSPI protocolSPI) {
		this.protocolSPI = protocolSPI;
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		ByteBuffer bb = buffer.toByteBuffer();
		
		ByteBuffer[] channelBuffers = null;
		
		int i = 0;
		
		for (; i < ServerConstants.BATCH_PROCESS_NUMBER; i++) {
			ByteBuffer o = decodeOne(bb);
			if (o == null){
				break;
			}else{
				buffer.skipBytes(o.remaining());
				if (channelBuffers == null){
					channelBuffers = new ByteBuffer[ServerConstants.BATCH_PROCESS_NUMBER];
				}
				channelBuffers[i] = o;
			}
		}
		
		if (i == 0){
			return null;
		}else{
			if (i < ServerConstants.BATCH_PROCESS_NUMBER){
				return Arrays.copyOf(channelBuffers, i);
			}else{
				return channelBuffers;
			}
		}

	}
	
	private ByteBuffer decodeOne(ByteBuffer bb){
		
		if (bb.remaining() == 0) {
			return null;
		}
		int oriPosi = bb.position();
		int messageLength = readMessageLength(bb);
		bb.position(oriPosi);

		// Make sure if the length field was received.
		if (messageLength <= 0) {
			// can not read msg length from header, not enough bytes
			return null;
		}

		// Make sure if there's enough bytes in the buffer.
		if (bb.remaining() < messageLength) {
			// The whole bytes were not received yet - return null.
			// This method will be invoked again when more packets are
			// received and appended to the buffer.

			// Reset to the marked position to read the length field again
			// next time.
			return null;
		}

		// There's enough bytes in the buffer. Read it.
		byte[] msgBytes = new byte[messageLength];
		bb.get(msgBytes);
		// Successfully decoded a frame. Return the decoded frame.
		return ByteBuffer.wrap(msgBytes);
	}
	

	public int readMessageLength(ByteBuffer in) {
		if (in.remaining() < protocolSPI.headerLength()) {
			return -1;
		}

		int value = protocolSPI.packageLength(in);

		return value;
	}

}
/**
 * 
 */
package com.shark.iopattern.touchstone.server.netty;

import java.nio.ByteBuffer;

import com.shark.iopattern.touchstone.share.Constants;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;

import com.shark.iopattern.touchstone.server.ServerSPI;


/**
 * @author weili1
 *
 */
@Sharable
public class NettyHandler extends SimpleChannelUpstreamHandler {
	
	ServerSPI serverSPI;
	
	public NettyHandler(ServerSPI serverSPI) {
		this.serverSPI = serverSPI;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		
		ByteBuffer[] received = (ByteBuffer[]) e.getMessage();

		for(ByteBuffer recvBuffer : received){
			
			ByteBuffer sendBuffer = ByteBuffer.allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
	
			serverSPI.process(recvBuffer, sendBuffer);
	
			ChannelFuture cf = Channels.write(ctx.getChannel(), ChannelBuffers.wrappedBuffer(sendBuffer));
		}
		
	}
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		super.channelOpen(ctx, e);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		super.exceptionCaught(ctx, e);
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		super.channelClosed(ctx, e);
		e.getChannel().close();
	}
	
}
/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.share.DefaultChannelHolder;
import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 * 
 */
public class SelectionKeyChannelHolder extends DefaultChannelHolder {
	private SelectionKey key;

	public SelectionKeyChannelHolder(SelectionKey key, SocketChannel channel,
			ProtocolSPI protocolSPI) {
		super(channel, protocolSPI, ServerConstants.READ_BUFFER_SIZE);
		this.key = key;
	}

	public void beNotifiedWhenReadable() {
		if (key != null) {

			int interestOps = key.interestOps();
			interestOps |= SelectionKey.OP_READ;
			key.interestOps(interestOps);

			// cycle the selector so this key is active again
			key.selector().wakeup();
			// logger.info("SelectionKey interest Read:" + ((interestOps &
			// SelectionKey.OP_READ) != 0));

		}
	}

	private void dispose() {
		if (key != null) {
			key.cancel();
		}
	}
	
	@Override
	public void close() {
		try {
			dispose();
			this.channel_.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

package org.apache.uniffle.common.netty.protocol;

import io.netty.channel.Channel;

public interface Transferable {

  void transferTo(Channel channel);
}

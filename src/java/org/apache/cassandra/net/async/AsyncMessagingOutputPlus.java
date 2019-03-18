/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.SharedDefaultFileRegion.SharedFileChannel;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A {@link DataOutputStreamPlus} that writes ASYNCHRONOUSLY to a Netty Channel.
 *
 * The close() and flush() methods synchronously wait for pending writes, and will propagate any exceptions
 * encountered in writing them to the wire.
 *
 * The correctness of this class depends on the ChannelPromise we create against a Channel always being completed,
 * which appears to be a guarantee provided by Netty so long as the event loop is running.
 */
public class AsyncMessagingOutputPlus extends AsyncChannelOutputPlus
{
    /**
     * the maximum {@link #highWaterMark} and minimum {@link #lowWaterMark} number of bytes we have flushing
     * during which we should still be writing to the channel.
     *
     * i.e., if we are at or below the {@link #lowWaterMark} we should definitely start writing again;
     *       if we are at or above the {@link #highWaterMark} we should definitely stop writing;
     *       if we are inbetween, it is OK to either write or not write
     *
     * note that we consider the bytes we are about to write to our high water mark, but not our low.
     * i.e., we will not begin a write that would take us over our high water mark, unless not doing so would
     * take us below our low water mark.
     *
     * This is somewhat arbitrary accounting, and a meaningless distinction for flushes of a consistent size.
     */
    private final int highWaterMark;
    private final int lowWaterMark;
    private final int bufferSize;

    private final FrameEncoder.PayloadAllocator payloadAllocator;
    private volatile FrameEncoder.Payload payload;

    public AsyncMessagingOutputPlus(Channel channel, int bufferSize, FrameEncoder.PayloadAllocator payloadAllocator)
    {
        super(channel);
        WriteBufferWaterMark waterMark = channel.config().getWriteBufferWaterMark();
        this.lowWaterMark = waterMark.low();
        this.highWaterMark = waterMark.high();
        this.bufferSize = bufferSize;
        this.payloadAllocator = payloadAllocator;
        allocateBuffer();
    }

    private void allocateBuffer()
    {
        payload = payloadAllocator.allocate(false, bufferSize);
        buffer = payload.buffer;
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        if (!channel.isOpen())
            throw new ClosedChannelException();

        // flush the current backing write buffer only if there's any pending data
        FrameEncoder.Payload flush = payload;
        int byteCount = flush.length();
        if (byteCount == 0)
            return;

        flush.finish();
        ChannelPromise promise = beginFlush(byteCount, lowWaterMark, highWaterMark);
        channel.writeAndFlush(flush, promise);
        allocateBuffer();
    }

    public long position()
    {
        return flushed() + payload.length();
    }

    /**
     * Discard any buffered data, and the buffers that contain it.
     * May be invoked instead of {@link #close()} if we terminate exceptionally.
     */
    public void discard()
    {
        if (payload != null)
        {
            payload.release();
            payload = null;
            buffer = null;
        }
    }
}
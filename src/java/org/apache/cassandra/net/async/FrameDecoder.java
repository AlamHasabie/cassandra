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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.net.Message;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

abstract class FrameDecoder extends ChannelInboundHandlerAdapter
{
    private static final FrameProcessor NO_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on an unregistered FrameDecoder"); };

    private static final FrameProcessor CLOSED_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on a closed FrameDecoder"); };

    interface FrameProcessor
    {
        /**
         * Frame processor that the frames should be handed off to.
         *
         * @return true if more frames can be taken by the processor, false if the decoder should pause until
         * it's explicitly resumed.
         */
        boolean process(Frame frame) throws IOException;
    }

    abstract static class Frame
    {
        final boolean isSelfContained;
        final int frameSize;

        Frame(boolean isSelfContained, int frameSize)
        {
            this.isSelfContained = isSelfContained;
            this.frameSize = frameSize;
        }

        abstract void release();
        abstract boolean isConsumed();
    }

    /**
     * The payload bytes of a complete frame, i.e. a frame stripped of its headers and trailers,
     * with any verification supported by the protocol confirmed.
     *
     * If {@code isSelfContained} the payload contains one or more {@link Message}, all of which
     * may be parsed entirely from the bytes provided.  Otherwise, only a part of exactly one
     * {@link Message} is contained in the payload; it can be relied upon that this partial {@link Message}
     * will only be delivered in its own unique {@link Frame}.
     */
    final static class IntactFrame extends Frame
    {
        final SharedBytes contents;

        IntactFrame(boolean isSelfContained, SharedBytes contents)
        {
            super(isSelfContained, contents.readableBytes());
            this.contents = contents;
        }

        void release()
        {
            contents.release();
        }

        boolean isConsumed()
        {
            return !contents.isReadable();
        }
    }

    /**
     * A corrupted frame was encountered; this represents the knowledge we have about this frame,
     * and whether or not the stream is recoverable.
     */
    final static class CorruptFrame extends Frame
    {
        final int readCRC, computedCRC;

        CorruptFrame(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            super(isSelfContained, frameSize);
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        static CorruptFrame recoverable(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            return new CorruptFrame(isSelfContained, frameSize, readCRC, computedCRC);
        }

        static CorruptFrame unrecoverable(int readCRC, int computedCRC)
        {
            return new CorruptFrame(false, Integer.MIN_VALUE, readCRC, computedCRC);
        }

        boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }

        void release() { }

        boolean isConsumed()
        {
            return true;
        }
    }

    protected final BufferPoolAllocator allocator;

    @VisibleForTesting
    final Deque<Frame> frames = new ArrayDeque<>(4);
    ByteBuffer stash;

    private boolean isActive;
    private Channel channel;
    private ChannelHandlerContext closed;
    private FrameProcessor processor = NO_PROCESSOR;

    FrameDecoder(BufferPoolAllocator allocator)
    {
        this.allocator = allocator;
    }

    abstract void decode(Collection<Frame> into, SharedBytes bytes);
    abstract void addLastTo(ChannelPipeline pipeline);

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to start receiving frames.
     */
    void activate(FrameProcessor processor)
    {
        if (this.processor != NO_PROCESSOR)
            throw new IllegalStateException("Attempted to activate an already active FrameDecoder");

        this.processor = processor;

        isActive = true;
        channel.read();
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     */
    void reactivate() throws IOException
    {
        if (isActive)
            throw new IllegalStateException("Tried to reactivate an already active FrameDecoder");

        if (deliver(processor))
        {
            isActive = true;
            onExhausted();
        }
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     *
     * Does not reactivate processing or reading from the wire, but permits processing as many frames (or parts thereof)
     * that are already waiting as the processor requires.
     */
    void processBacklog(FrameProcessor processor) throws IOException
    {
        deliver(processor);
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to permanently
     * stop receiving frames, e.g. because of an exception caught.
     */
    void discard()
    {
        isActive = false;
        processor = CLOSED_PROCESSOR;
        if (stash != null)
        {
            ByteBuffer bytes = stash;
            stash = null;
            allocator.put(bytes);
        }
        while (!frames.isEmpty())
            frames.poll().release();
    }

    /**
     * Called by Netty pipeline when a new message arrives; we anticipate in normal operation
     * this will receive messages of type {@link GlobalBufferPoolAllocator.Wrapped} or
     * {@link LocalBufferPoolAllocator.Wrapped}.
     *
     * These buffers are unwrapped and passed to {@link #decode(Collection, SharedBytes)},
     * which collects decoded frames into {@link #frames}, which we send upstream in {@link #deliver}
     */
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        if (msg instanceof BufferPoolAllocator.Wrapped)
        {
            ByteBuffer buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
            // netty will probably have mis-predicted the space needed
            allocator.putUnusedPortion(buf, false);
            channelRead(SharedBytes.wrap(buf));
        }
        else if (msg instanceof SharedBytes) // legacy LZ4 decoder
        {
            channelRead((SharedBytes) msg);
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }

    private void channelRead(SharedBytes bytes) throws IOException
    {
        decode(frames, bytes);

        if (isActive) isActive = deliver(processor);
    }

    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        if (isActive)
            onExhausted();
    }

    /**
     * Only to be invoked when frames.isEmpty().
     *
     * If we have been closed, we will now propagate up the channelInactive notification,
     * and otherwise we will ask the channel for more data.
     */
    private void onExhausted()
    {
        if (isClosed())
            close();
        else
            channel.read();
    }

    /**
     * Deliver any waiting frames, including those that were incompletely read last time, to the provided processor
     * until the processor returns {@code false}, or we finish the backlog.
     *
     * Propagate the final return value of the processor.
     */
    private boolean deliver(FrameProcessor processor) throws IOException
    {
        boolean deliver = true;
        while (deliver && !frames.isEmpty())
        {
            Frame frame = frames.peek();
            deliver = processor.process(frame);

            assert !deliver || frame.isConsumed();
            if (deliver || frame.isConsumed())
            {
                frames.poll();
                frame.release();
            }
        }
        return deliver;
    }

    void stash(SharedBytes in, int stashLength, int begin, int length)
    {
        ByteBuffer out = allocator.getAtLeast(stashLength);
        copyBytes(in.get(), begin, out, 0, length);
        out.position(length);
        stash = out;
    }


    public void handlerAdded(ChannelHandlerContext ctx)
    {
        channel = ctx.channel();
        channel.config().setAutoRead(false);
    }

    public void channelInactive(ChannelHandlerContext ctx)
    {
        closed = ctx;
        if (frames.isEmpty())
            close();
        allocator.release();
    }

    private boolean isClosed()
    {
        return closed != null;
    }

    private void close()
    {
        discard();
        channel = null;
        closed.fireChannelInactive();
        closed = null;
    }

    /**
     * Utility: fill {@code out} from {@code in} up to {@code toOutPosition},
     * updating the position of both buffers with the result
     * @return true if there were sufficient bytes to fill to {@code toOutPosition}
     */
    static boolean copyToSize(ByteBuffer in, ByteBuffer out, int toOutPosition)
    {
        int bytesToSize = toOutPosition - out.position();
        if (bytesToSize <= 0)
            return true;

        if (bytesToSize > in.remaining())
        {
            out.put(in);
            return false;
        }

        copyBytes(in, in.position(), out, out.position(), bytesToSize);
        in.position(in.position() + bytesToSize);
        out.position(toOutPosition);
        return true;
    }

    /**
     * @return {@code in} if has sufficient capacity, otherwise
     *         a replacement from {@code BufferPool} that {@code in} is copied into
     */
    ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
    {
        if (in.capacity() >= capacity)
            return in;

        ByteBuffer out = allocator.getAtLeast(capacity);
        in.flip();
        out.put(in);
        allocator.put(in);
        return out;
    }
}

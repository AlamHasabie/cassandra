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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.InvalidLegacyProtocolMagic;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.Crc.InvalidCrc;
import org.apache.cassandra.net.async.FrameDecoder.Frame;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.Slice;
import org.apache.cassandra.net.async.InboundCallbacks.MessageProcessor;
import org.apache.cassandra.net.async.InboundCallbacks.OnHandlerClosed;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageError;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageExpired;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageProcessed;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
import org.apache.cassandra.net.async.ResourceLimits.Outcome;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.min;

/**
 * Parses incoming messages as per the 3.0/3.11/4.0 internode messaging protocols.
 */
public final class InboundMessageHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private enum State { ACTIVE, AHEAD_OF_COPROCESSORS, OVER_RESERVE_CAPACITY, CLOSED }
    private State state = State.ACTIVE;

    private final Channel channel;
    private final InetAddressAndPort peer;
    private final int version;

    private final int largeThreshold;
    private final ExecutorService largeExecutor;

    private final long queueCapacity;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<InboundMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandler.class, "queueSize");

    private final Limit endpointReserveCapacity;
    private final WaitQueue endpointWaitQueue;

    private final Limit globalReserveCapacity;
    private final WaitQueue globalWaitQueue;

    private final OnMessageError onError;
    private final OnMessageExpired onExpired;
    private final OnMessageProcessed onProcessed;
    private final OnHandlerClosed onClosed;

    private final MessageProcessor processor;

    private int largeBytesRemaining; // remainig bytes we need to supply to the coprocessor to deserialize the in-flight large message
    private int skipBytesRemaining;  // remaining bytes we need to skip to get over the expired message

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long receivedCount, receivedBytes; // intentionally not volatile

    private LargeCoprocessor largeCoprocessor;

    private volatile int largeUnconsumedBytes; // unconsumed bytes in all ByteBufs queued up in all coprocessors
    private static final AtomicIntegerFieldUpdater<InboundMessageHandler> largeUnconsumedBytesUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandler.class, "largeUnconsumedBytes");

    InboundMessageHandler(Channel channel,
                          InetAddressAndPort peer,
                          int version,

                          int largeThreshold,
                          ExecutorService largeExecutor,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnMessageError onError,
                          OnMessageExpired onExpired,
                          OnMessageProcessed onProcessed,
                          OnHandlerClosed onClosed,

                          MessageProcessor processor)
    {
        this.channel = channel;
        this.peer = peer;
        this.version = version;

        this.largeThreshold = largeThreshold;
        this.largeExecutor = largeExecutor;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onError = onError;
        this.onExpired = onExpired;
        this.onProcessed = onProcessed;
        this.onClosed = onClosed;

        this.processor = processor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InvalidLegacyProtocolMagic, InvalidCrc
    {
        if (isClosed())
            Frame.release(msg);
        else if (msg instanceof CorruptFrame)
            handleCorruption((CorruptFrame) msg);
        else
            doChannelRead((IntactFrame) msg);
    }

    private void doChannelRead(IntactFrame frame) throws InvalidLegacyProtocolMagic
    {
        final Slice slice = frame.contents;
        final int readableBytes = slice.contents.remaining();

        // some bytes of an expired message in the stream to skip still
        if (skipBytesRemaining > 0)
        {
            int skippedBytes = min(readableBytes, skipBytesRemaining);
            receivedBytes += skippedBytes;
            slice.skipBytes(skippedBytes);

            skipBytesRemaining -= skippedBytes;
            if (skipBytesRemaining > 0)
            {
                slice.release();
            }
            else
            {
                receivedCount++;
                processMessages(slice);
            }
        }

        // no large message in-flight
        else if (largeBytesRemaining == 0)
        {
            processMessages(unstash(slice));
        }

        // less than enough bytes to complete the large message in-flight
        else if (readableBytes < largeBytesRemaining)
        {
            receivedBytes += readableBytes;
            largeBytesRemaining -= readableBytes;
            if (!largeCoprocessor.supply(slice))
                aheadOfCoprocessors();
        }

        // just enough bytes to complete the large message in-flight
        else if (readableBytes == largeBytesRemaining)
        {
            receivedCount++;
            receivedBytes += largeBytesRemaining;

            largeBytesRemaining = 0;
            if (!largeCoprocessor.supply(slice))
                aheadOfCoprocessors();
            closeCoprocessor();
        }

        // more than enough bytes to complete the large message in-flight
        else
        {
            receivedCount++;
            receivedBytes += largeBytesRemaining;

            boolean isKeepingUp = largeCoprocessor.supply(slice.sliceAndConsume(largeBytesRemaining));
            slice.skipBytes(largeBytesRemaining);
            largeBytesRemaining = 0;
            closeCoprocessor();

            if (!isKeepingUp)
                aheadOfCoprocessors();

            if (isKeepingUp)
                processMessages(slice);
            else
                stash(slice);
        }
    }

    /*
     * Handle a corrupted LZ4 frame.
     */
    private void handleCorruption(CorruptFrame frame) throws InvalidCrc
    {
        if (!frame.isRecoverable())
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);

        final int frameSize = frame.frameSize;
        receivedBytes += frameSize;

        /*
         * Skipping bytes in the middle or at the very end of a large message (small messages never cross lz4 frame boundary)
         */
        if (skipBytesRemaining > 0)
        {
            if (skipBytesRemaining == frameSize)
                receivedCount++;
            skipBytesRemaining -= frameSize;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a large message from {}", peer);
        }
        /*
         * Corrupted frame in the middle or at the very end of a large message. Can safely be skipped with only the
         * large message in flight dropped.
         *
         * Close the coprocessor (which will, on close, handle resource release and onError metric update), and switch
         * to skipping the remaining bytes.
         */
        else if (largeBytesRemaining > 0)
        {
            if (largeBytesRemaining == frameSize)
                receivedCount++;
            closeCoprocessor();
            skipBytesRemaining = largeBytesRemaining - frameSize;
            largeBytesRemaining = 0;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a large message from {}", peer);
        }
        /*
         * First frame of a large message, or a frame of small messages; nothing we can really do here.
         *
         * If it's the former, then we'll fail to parse the second frame of a large message whenever it arrives,
         * and close the channel then.
         *
         * If it's the latter, then all the messages in the frame will have to be dropped,
         * but the following frame, barring further corruption, will be parsed just fine.
         */
        else
        {
            if (frame.isSelfContained)
            {
                noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading messages from {} (corrupted frame of small messages)", peer);
            }
            else
            {
                noSpamLogger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} (corrupted first frame of a large message)", peer);
                throw new InvalidCrc(frame.readCRC, frame.computedCRC);
            }
        }
    }

    private void processMessages(Slice slice) throws InvalidLegacyProtocolMagic
    {
        processMessages(slice, Integer.MAX_VALUE, endpointReserveCapacity, globalReserveCapacity);
    }

    /*
     * Process a stream of messages (potentially not ending with a completely read one). The buffer starts at a boundary
     * of a new message. Will process at most count messages.
     */
    private void processMessages(Slice slice, int count, Limit endpointReserve, Limit globalReserve) throws InvalidLegacyProtocolMagic
    {
        try
        {
            //noinspection StatementWithEmptyBody
            while (count-- > 0 && processMessage(slice, endpointReserve, globalReserve));
        }
        finally
        {
            if (slice.contents.hasRemaining())
                stash(slice);
            else
                slice.release();
        }
    }

    private boolean processMessage(Slice slice, Limit endpointReserve, Limit globalReserve) throws InvalidLegacyProtocolMagic
    {
        ByteBuffer buf = slice.contents;
        int messageSize = serializer.messageSize(buf, version);

        if (messageSize < 0) // not enough bytes to read size of the message
            return false;

        /*
         * if we can calculate message size, then at the very least we have enough bytes to read the header fully
         */

        long currentTimeNanos = ApproximateTime.nanoTime();
        long createdAtNanos = serializer.getCreatedAtNanos(buf, peer, version);
        long expiresAtNanos = serializer.getExpiresAtNanos(buf, createdAtNanos, version);

        if (expiresAtNanos < currentTimeNanos)
        {
            onExpired.call(serializer.getVerb(buf, version), messageSize, currentTimeNanos - createdAtNanos, TimeUnit.NANOSECONDS);

            int skippedBytes = min(buf.remaining(), messageSize);
            receivedBytes += skippedBytes;
            slice.skipBytes(skippedBytes);

            skipBytesRemaining = messageSize - skippedBytes;
            if (skipBytesRemaining == 0)
                receivedCount++;
            return buf.hasRemaining();
        }

        Outcome outcome = acquireCapacity(endpointReserve, globalReserve, messageSize);
        if (outcome != Outcome.SUCCESS)
        {
            overReserveCapacity(messageSize, expiresAtNanos, outcome);
            return false;
        }

        return messageSize <= largeThreshold
             ? processSmallMessage(buf, messageSize)
             : processLargeMessage(slice, messageSize);
    }

    @SuppressWarnings({ "resource", "RedundantSuppression" })
    private boolean processSmallMessage(ByteBuffer buf, int messageSize)
    {
        if (buf.remaining() < messageSize)
            return false; // not enough bytes to read the entire small message

        receivedCount += 1;
        receivedBytes += messageSize;

        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + messageSize); // cap to expected message size

        Message<?> message = null;
        try
        {
            message = serializer.deserialize(new DataInputBuffer(buf, false), peer, version);
        }
        catch (UnknownTableException e)
        {
            noSpamLogger.info("UnknownTableException caught while reading a small message from {}: {}", peer, e);
            onError.call(e, messageSize);
        }
        catch (IOException e)
        {
            logger.error("Unexpected IOException caught while reading a small message from " + peer, e);
            onError.call(e, messageSize);
        }
        catch (Throwable t)
        {
            releaseCapacity(messageSize);
            onError.call(t, messageSize);
            throw t;
        }
        finally
        {
            buf.position(begin + messageSize);
            buf.limit(end);
        }

        if (null != message)
            processor.process(message, messageSize, this::onMessageProcessed, this::onMessageExpired);
        else
            releaseCapacity(messageSize);

        return buf.hasRemaining();
    }

    private boolean processLargeMessage(Slice slice, int messageSize)
    {
        largeCoprocessor = new LargeCoprocessor(messageSize);
        largeExecutor.submit(largeCoprocessor);

        boolean isKeepingUp;
        final int readableBytes = slice.readableBytes();

        // not enough bytes for the large message
        if (readableBytes < messageSize)
        {
            receivedBytes += readableBytes;
            isKeepingUp = largeCoprocessor.supply(slice.sliceAndConsume(readableBytes));
            largeBytesRemaining = messageSize - readableBytes;
        }
        // just enough bytes for the large message
        else if (readableBytes == messageSize)
        {
            receivedCount++;
            receivedBytes += readableBytes;

            isKeepingUp = largeCoprocessor.supply(slice.sliceAndConsume(readableBytes));
            largeBytesRemaining = 0;
            closeCoprocessor();
        }
        // more than enough bytes for the large message
        else
        {
            receivedCount++;
            receivedBytes += messageSize;

            isKeepingUp = largeCoprocessor.supply(slice.sliceAndConsume(messageSize));
            slice.skipBytes(messageSize);
            closeCoprocessor();
        }

        if (!isKeepingUp)
            aheadOfCoprocessors();

        return isKeepingUp && slice.isReadable();
    }

    // wrap parent callback to release capacity first
    private void onMessageProcessed(int messageSize)
    {
        releaseCapacity(messageSize);
        onProcessed.call(messageSize);
    }

    // wrap parent callback to release capacity first
    private void onMessageExpired(Verb verb, int messageSize, long timeElapsed, TimeUnit unit)
    {
        releaseCapacity(messageSize);
        onExpired.call(verb, messageSize, timeElapsed, unit);
    }

    private void aheadOfCoprocessors()
    {
        state = State.AHEAD_OF_COPROCESSORS;
        pauseReading();
    }

    private void overReserveCapacity(int messageSize, long expiresAtNanos, Outcome failureReason)
    {
        if (failureReason == Outcome.INSUFFICIENT_ENDPOINT)
        {
            ticket = endpointWaitQueue.registerAndSignal(
                messageSize, expiresAtNanos, channel.eventLoop(), this::onEndpointReserveCapacityRegained, this::processStash);
        }
        else
        {
            ticket = globalWaitQueue.registerAndSignal(
                messageSize, expiresAtNanos, channel.eventLoop(), this::onGlobalReserveCapacityRegained, this::processStash);
        }

        state = State.OVER_RESERVE_CAPACITY;
        pauseReading();
    }

    // left over bytes that couldn't be processed yet for whatever reason (incomplete, or got ahead of coprocessor or limits)
    private ByteBuffer stashedBytes;

    private Slice unstash()
    {
        ByteBuffer buf = stashedBytes;
        if (buf == null)
            return null;
        stashedBytes = null;
        return Slice.wrap(buf);
    }

    private Slice unstash(Slice append)
    {
        ByteBuffer buf = stashedBytes;
        if (buf == null)
            return append;

        stashedBytes = null;

        int savePosition = readableToWriteable(buf);
        if (buf.remaining() < append.readableBytes())
        {
            writableToReadable(buf, savePosition);
            ByteBuffer newBuffer = BufferPool.get(buf.remaining() + append.readableBytes());
            newBuffer.put(buf);
            BufferPool.put(buf);
            buf = newBuffer;
            savePosition = 0;
        }

        buf.put(append.contents);
        append.release();
        writableToReadable(buf, savePosition);

        return Slice.wrap(buf);
    }

    private static int readableToWriteable(ByteBuffer buffer)
    {
        int position = buffer.position();
        buffer.position(buffer.limit());
        buffer.limit(buffer.capacity());
        return position;
    }

    private static void writableToReadable(ByteBuffer buffer, int position)
    {
        buffer.limit(buffer.position());
        buffer.position(position);
    }

    private void stash(Slice slice)
    {
        stashedBytes = slice.tryAdopt();
        if (stashedBytes == null)
        {
            try
            {
                stashedBytes = BufferPool.get(slice.readableBytes(), BufferType.OFF_HEAP);
                stashedBytes.put(slice.contents);
                stashedBytes.flip();
            }
            finally
            {
                slice.release();
            }
        }
    }

    private void onCoprocessorCaughtUp()
    {
        resumeProcessing(Integer.MAX_VALUE, endpointReserveCapacity, globalReserveCapacity);
    }

    private void onEndpointReserveCapacityRegained(Limit endpointReserve)
    {
        ticket = null;
        resumeProcessing(1, endpointReserve, globalReserveCapacity);
    }

    private void onGlobalReserveCapacityRegained(Limit globalReserve)
    {
        ticket = null;
        resumeProcessing(1, endpointReserveCapacity, globalReserve);
    }

    private void resumeProcessing(int maxCount, Limit endpointReserve, Limit globalReserve)
    {
        assert channel.eventLoop().inEventLoop();

        if (isClosed())
            return;

        state = State.ACTIVE;

        // resume from where we left off
        Slice stash = unstash();
        try
        {
            if (null != stash)
                processMessages(stash, maxCount, endpointReserve, globalReserve);
        }
        catch (InvalidLegacyProtocolMagic e)
        {
            exceptionCaught(e);
        }

        if (isActive())      // processBuffer() may have hit another roadblock,
            resumeReading(); // so only resume reading if we are still good to go
    }

    private void processStash()
    {
        assert channel.eventLoop().inEventLoop();

        if (isClosed())
            return;

        // resume from where we left off
        Slice stash = unstash();
        try
        {
            if (null != stash)
                processMessages(stash);
        }
        catch (InvalidLegacyProtocolMagic e)
        {
            exceptionCaught(e);
        }
    }

    private Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return Outcome.SUCCESS;
        }

        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);
        Outcome outcome = ResourceLimits.tryAllocate(endpointReserve, globalReserve, allocatedExcess);
        if (outcome != Outcome.SUCCESS)
            return outcome;

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = min(newQueueSize - queueCapacity, bytes);

        if (actualExcess != allocatedExcess) // can be smaller if a release happened since
            ResourceLimits.release(endpointReserve, globalReserve, allocatedExcess - actualExcess);

        return Outcome.SUCCESS;
    }

    private void releaseCapacity(int bytes)
    {
        long oldQueueSize = queueSizeUpdater.getAndAdd(this, -bytes);
        if (oldQueueSize > queueCapacity)
        {
            long excess = min(oldQueueSize - queueCapacity, bytes);
            ResourceLimits.release(endpointReserveCapacity, globalReserveCapacity, excess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }
    }

    private void pauseReading()
    {
        channel.config().setAutoRead(false);
    }

    private void resumeReading()
    {
        channel.config().setAutoRead(true);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        exceptionCaught(cause);
    }

    private void exceptionCaught(Throwable cause)
    {
        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} - closing the connection", peer);
        else
            logger.error("Unexpected exception caught while processing inbound messages from " + peer + "; terminating connection", cause);

        channel.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    /*
     * Clean up after ourselves
     */
    private void close()
    {
        state = State.CLOSED;

        Slice stash = unstash();
        if (null != stash)
            stash.release();

        if (null != largeCoprocessor)
            closeCoprocessor();

        if (null != ticket)
        {
            ticket.invalidate();
            ticket = null;
        }

        onClosed.call(this);
    }

    private boolean isActive()
    {
        return state == State.ACTIVE;
    }

    private boolean isClosed()
    {
        return state == State.CLOSED;
    }

    private void closeCoprocessor()
    {
        largeCoprocessor.close();
        largeCoprocessor = null;
    }

    /**
     * This will execute on a thread that isn't a netty event loop.
     */
    private final class LargeCoprocessor implements Runnable
    {
        private final int messageSize;
        private final AsyncInputPlus input;

        private final int maxUnconsumedBytes;

        private LargeCoprocessor(int messageSize)
        {
            this.messageSize = messageSize;

            this.input = new AsyncInputPlus(this::onBufConsumed);


            /*
             * Allow up to 2x large message threshold bytes of ByteBufs in coprocessors' queues before pausing reads
             * from the channel. Signal the handler to resume reading from the channel once we've consumed enough
             * bytes from the queues to drop below this threshold again.
             */
            maxUnconsumedBytes = largeThreshold * 2;
        }

        public void run()
        {
            String priorThreadName = null;
            try
            {
                priorThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName("MessagingService-Inbound-" + peer + "-LargeMessage-" + messageSize);

                processLargeMessage();
            }
            finally
            {
                if (null != priorThreadName)
                    Thread.currentThread().setName(priorThreadName);
            }
        }

        private void processLargeMessage()
        {
            Message<?> message = null;
            try
            {
                message = serializer.deserialize(input, peer, version);
            }
            catch (AsyncInputPlus.InputClosedException e)
            {
                /*
                 * Closure was requested from the event loop, before we could deserialize the message fully;
                 * we are done here, and AIP will have closed itself.
                 */
                onError.call(e, messageSize);
            }
            catch (UnknownTableException e)
            {
                noSpamLogger.info("UnknownTableException caught while reading a large message from {}: {}", peer, e);
                onError.call(e, messageSize);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Unexpected exception caught while reading a large message from " + peer, t);
                onError.call(t, messageSize);
            }
            finally
            {
                input.close();
            }

            if (null != message)
                processor.process(message, messageSize, InboundMessageHandler.this::onMessageProcessed, InboundMessageHandler.this::onMessageExpired);
            else
                releaseCapacity(messageSize);
        }

        void close()
        {
            input.requestClosure();
        }

        /*
         * Returns true if coprocessor is keeping up and can accept more input, false if it's fallen behind.
         */
        boolean supply(Slice slice)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, slice.readableBytes());
            input.supply(slice);
            return unconsumed <= maxUnconsumedBytes;
        }

        private void onBufConsumed(int size)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, -size);
            int prevUnconsumed = unconsumed + size;

            if (unconsumed <= maxUnconsumedBytes && prevUnconsumed > maxUnconsumedBytes)
                channel.eventLoop().submit(InboundMessageHandler.this::onCoprocessorCaughtUp);
        }
    }

    public static final class WaitQueue
    {
        /*
         * Callback scheduler states
         */
        private static final int NOT_RUNNING = 0;
        @SuppressWarnings("unused")
        private static final int RUNNING     = 1;
        private static final int RUN_AGAIN   = 2;
        private volatile int scheduled;
        private static final AtomicIntegerFieldUpdater<WaitQueue> scheduledUpdater =
            AtomicIntegerFieldUpdater.newUpdater(WaitQueue.class, "scheduled");

        private final Limit reserveCapacity;

        private final ConcurrentLinkedQueue<Ticket> queue = new ConcurrentLinkedQueue<>();

        public WaitQueue(Limit reserveCapacity)
        {
            this.reserveCapacity = reserveCapacity;
        }

        private Ticket registerAndSignal(int bytesRequested,
                                         long expiresAtNanos,
                                         EventLoop eventLoop,
                                         Consumer<Limit> processOneCallback,
                                         Runnable processStashCallback)
        {
            Ticket ticket = new Ticket(this, bytesRequested, expiresAtNanos, eventLoop, processOneCallback, processStashCallback);
            queue.add(ticket);
            signal();
            return ticket;
        }

        void signal()
        {
            if (queue.isEmpty())
                return;

            if (NOT_RUNNING == scheduledUpdater.getAndUpdate(this, i -> Integer.min(RUN_AGAIN, i + 1)))
            {
                do
                {
                    schedule();
                }
                while (RUN_AGAIN == scheduledUpdater.getAndDecrement(this));
            }
        }

        private void schedule()
        {
            Map<EventLoop, ResumeProcessing> tasks = null;

            long nanoTime = ApproximateTime.nanoTime();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    queue.poll();
                    continue;
                }

                if (t.isLive(nanoTime) && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    t.reset();
                    break;
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                tasks.computeIfAbsent(t.eventLoop, e -> new ResumeProcessing()).add(queue.poll());
            }

            if (null != tasks)
                tasks.forEach(EventExecutorGroup::submit);
        }

        class ResumeProcessing implements Runnable
        {
            List<Ticket> tickets = new ArrayList<>();

            private void add(Ticket ticket)
            {
                tickets.add(ticket);
            }

            public void run()
            {
                long capacity = 0L;

                for (Ticket t : tickets)
                    capacity += t.bytesRequested;

                Limit limit = new ResourceLimits.Basic(capacity);
                try
                {
                    tickets.forEach(t -> t.processOneCallback.accept(limit));
                }
                finally
                {
                    /*
                     * Free up any unused global capacity, if any. Will be non-zero if one or more handlers were closed
                     * when we attempted to run their callback or used more of their personal allowance.
                     */
                    reserveCapacity.release(limit.remaining());
                }

                // for every handler, next attempt parsing remaining stashed buf, if any, in their original queue order
                tickets.forEach(t -> t.processStashCallback.run());
            }
        }

        static final class Ticket
        {
            private static final int WAITING     = 0;
            private static final int CALLED      = 1;
            private static final int INVALIDATED = 2;

            private volatile int state;
            private static final AtomicIntegerFieldUpdater<Ticket> stateUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Ticket.class, "state");

            private final WaitQueue waitQueue;
            private final int bytesRequested;
            private final long expiresAtNanos;
            private final EventLoop eventLoop;
            private final Consumer<Limit> processOneCallback;
            private final Runnable processStashCallback;

            private Ticket(WaitQueue waitQueue,
                           int bytesRequested,
                           long expiresAtNanos,
                           EventLoop eventLoop,
                           Consumer<Limit> processOneCallback,
                           Runnable processStashCallback)
            {
                this.waitQueue = waitQueue;
                this.bytesRequested = bytesRequested;
                this.expiresAtNanos = expiresAtNanos;
                this.eventLoop = eventLoop;
                this.processOneCallback = processOneCallback;
                this.processStashCallback = processStashCallback;
            }

            boolean isInvalidated()
            {
                return state == INVALIDATED;
            }

            boolean isLive(long currentTimeNanos)
            {
                return currentTimeNanos <= expiresAtNanos;
            }

            void invalidate()
            {
                if (stateUpdater.compareAndSet(this, WAITING, INVALIDATED))
                    waitQueue.signal();
            }

            private boolean call()
            {
                return stateUpdater.compareAndSet(this, WAITING, CALLED);
            }

            private void reset()
            {
                state = WAITING;
            }
        }
    }
}

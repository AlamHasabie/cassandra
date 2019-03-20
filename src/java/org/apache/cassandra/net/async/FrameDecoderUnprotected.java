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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.net.async.FrameDecoderCrc.HEADER_LENGTH;
import static org.apache.cassandra.net.async.FrameDecoderCrc.isSelfContained;
import static org.apache.cassandra.net.async.FrameDecoderCrc.payloadLength;
import static org.apache.cassandra.net.async.FrameDecoderCrc.readHeader6b;
import static org.apache.cassandra.net.async.FrameDecoderCrc.verifyHeader6b;

public final class FrameDecoderUnprotected extends FrameDecoder
{
    public static FrameDecoderUnprotected create()
    {
        return new FrameDecoderUnprotected();
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        return readHeader6b(frame, begin);
    }

    final CorruptFrame verifyHeader(long header6b)
    {
        return verifyHeader6b(header6b);
    }

    final int frameLength(long header6b)
    {
        return payloadLength(header6b) + HEADER_LENGTH;
    }

    final Frame unpackFrame(SharedBytes bytes, int begin, int end, long header6b, boolean ownsBytes)
    {
        try
        {
            boolean isSelfContained = isSelfContained(header6b);
            bytes = slice(bytes, begin + HEADER_LENGTH, end, ownsBytes);
            ownsBytes = false;
            return new IntactFrame(isSelfContained, bytes);
        }
        finally
        {
            if (ownsBytes)
                bytes.release();
        }
    }

    void decode(Consumer<Frame> consumer, SharedBytes bytes)
    {
        decode(consumer, bytes, HEADER_LENGTH);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderUnprotected", this);
    }
}

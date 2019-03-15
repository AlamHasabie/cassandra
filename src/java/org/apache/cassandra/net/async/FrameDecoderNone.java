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
import java.util.List;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

@ChannelHandler.Sharable
class FrameDecoderNone extends FrameDecoder
{
    public static final FrameDecoderNone instance = new FrameDecoderNone();

    long readHeader(ByteBuffer in, int begin)
    {
        throw new UnsupportedOperationException();
    }

    CorruptFrame verifyHeader(long header)
    {
        throw new UnsupportedOperationException();
    }

    int frameLength(long header)
    {
        throw new UnsupportedOperationException();
    }

    Frame unpackFrame(Slice slice, int begin, int end, long header)
    {
        throw new UnsupportedOperationException();
    }

    void decode(ChannelHandlerContext ctx, Slice slice, List<Object> out)
    {
        out.add(new IntactFrame(false, slice));
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderNone", this);
    }
}

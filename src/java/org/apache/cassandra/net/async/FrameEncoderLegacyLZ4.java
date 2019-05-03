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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

@ChannelHandler.Sharable
class FrameEncoderLegacyLZ4 extends FrameEncoderLegacy
{
    public static final FrameEncoderLegacyLZ4 instance = new FrameEncoderLegacyLZ4();

    private static final int LEGACY_COMPRESSION_BLOCK_SIZE = 1 << 15;
    private static final int LEGACY_LZ4_HASH_SEED = 0x9747b28c;

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("legacyLz4", new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, LEGACY_COMPRESSION_BLOCK_SIZE, XXHashFactory.fastestInstance().newStreamingHash32(LEGACY_LZ4_HASH_SEED).asChecksum()));
        pipeline.addLast("frameEncoderNone", this);
    }
}
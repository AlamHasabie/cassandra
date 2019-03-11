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

package org.apache.cassandra.net;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;

@RunWith(Parameterized.class)
public class MessageInProcessorTest
{
    private static InetAddressAndPort addr;
    private final int messagingVersion;

    private ByteBuf buf;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByName("127.0.0.1");
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Parameters()
    public static Iterable<?> generateData()
    {
        return Arrays.asList(MessagingService.VERSION_30, MessagingService.VERSION_40);
    }

    public MessageInProcessorTest(int messagingVersion)
    {
        this.messagingVersion = messagingVersion;
    }

    @Test(expected = IOException.class)
    public void readFirstChunk_BadMagic() throws IOException
    {
        int len = Message.PRE_40_MESSAGE_PREFIX_SIZE;
        buf = Unpooled.buffer(len, len);
        buf.writeInt(-1);
        buf.writerIndex(len);

        Message.serializer.deserialize(new ByteBufDataInputPlus(buf), addr, messagingVersion);
    }
}
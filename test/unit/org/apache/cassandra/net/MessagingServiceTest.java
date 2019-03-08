/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.*;
import java.util.regex.Matcher;

import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;

import com.codahale.metrics.Timer;

import io.netty.channel.Channel;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.async.InboundConnectionSettings;
import org.apache.cassandra.net.async.InboundConnections;
import org.apache.cassandra.net.async.OutboundConnectionInitiator;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionSettings;
import org.apache.cassandra.utils.FBUtilities;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.EmptyMessage.emptyMessage;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;
import static org.junit.Assert.*;

public class MessagingServiceTest
{
    private final static long ONE_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();
    public static final IInternodeAuthenticator ALLOW_NOTHING_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort)
        {
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };
    private static IInternodeAuthenticator originalAuthenticator;
    private static ServerEncryptionOptions originalServerEncryptionOptions;
    private static InetAddressAndPort originalListenAddress;

    private final MessagingService messagingService = new MessagingService(true);

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBackPressureStrategy(new MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();
        originalServerEncryptionOptions = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        originalListenAddress = InetAddressAndPort.getByAddressOverrideDefaults(DatabaseDescriptor.getListenAddress(), DatabaseDescriptor.getStoragePort());
    }

    private static int metricScopeId = 0;

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.droppedMessages.resetMap(Integer.toString(metricScopeId++));
        MockBackPressureStrategy.applied = false;
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.2"));
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.3"));
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(originalServerEncryptionOptions);
        DatabaseDescriptor.setShouldListenOnBroadcastAddress(false);
        DatabaseDescriptor.setListenAddress(originalListenAddress.address);
        FBUtilities.reset();
    }

    @Test
    public void testDroppedMessages()
    {
        Verb verb = Verb.READ_REQ;

        for (int i = 1; i <= 5000; i++)
            messagingService.droppedMessages.incrementWithLatency(verb, i, MILLISECONDS, i % 2 == 0);

        List<String> logs = messagingService.droppedMessages.getLogs();
        assertEquals(1, logs.size());
        Pattern regexp = Pattern.compile("READ_REQ messages were dropped in last 5000 ms: (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms");
        Matcher matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(2500, Integer.parseInt(matcher.group(1)));
        assertEquals(2500, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(5000, (int) messagingService.droppedMessages.getDroppedMessages().get(verb.toString()));

        logs = messagingService.droppedMessages.getLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.droppedMessages.incrementWithLatency(verb, i, MILLISECONDS, i % 2 == 0);

        logs = messagingService.droppedMessages.getLogs();
        assertEquals(1, logs.size());
        matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(1250, Integer.parseInt(matcher.group(1)));
        assertEquals(1250, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(7500, (int) messagingService.droppedMessages.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;
        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency() throws Exception
    {
        // if clocks are off should just not track anything
        int latency = -100;

        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;

        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNull(dcLatency.get("datacenter1"));
    }

    @Test
    public void testQueueWaitLatency() throws Exception
    {
        int latency = 100;
        Verb verb = Verb.MUTATION_REQ;

        ConcurrentHashMap<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency, MILLISECONDS);
        assertNotNull(queueWaitLatency.get(verb));
        assertEquals(1, queueWaitLatency.get(verb).getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, queueWaitLatency.get(verb).getSnapshot().getMax());
    }

    @Test
    public void testNegativeQueueWaitLatency() throws Exception
    {
        int latency = -100;
        Verb verb = Verb.MUTATION_REQ;

        ConcurrentHashMap<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency, MILLISECONDS);
        assertNull(queueWaitLatency.get(verb));
    }

    @Test
    public void testUpdatesBackPressureOnSendWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddressAndPort.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        Message<?> ignored = null;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddressAndPort.getByName("127.0.0.2"), noCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, ignored);
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddressAndPort.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, timeout);
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }

    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddressAndPort.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddressAndPort.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddressAndPort.getByName("127.0.0.2")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddressAndPort.getByName("127.0.0.2")), ONE_SECOND);
        assertTrue(MockBackPressureStrategy.applied);
    }

    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddressAndPort.getByName("127.0.0.1")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);
    }

    private static void addDCLatency(long sentAt, long nowTime) throws IOException
    {
        Message.serializer.calculateCreationTimeNanos(InetAddressAndPort.getLocalHost(), (int) sentAt, nowTime);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(Set<MockBackPressureState> states, long timeout, TimeUnit unit)
        {
            if (!Iterables.isEmpty(states))
                applied = true;
        }

        @Override
        public MockBackPressureState newState(InetAddressAndPort host)
        {
            return new MockBackPressureState(host);
        }

        public static class MockBackPressureState implements BackPressureState
        {
            private final InetAddressAndPort host;
            public volatile boolean onSend = false;
            public volatile boolean onReceive = false;
            public volatile boolean onTimeout = false;

            private MockBackPressureState(InetAddressAndPort host)
            {
                this.host = host;
            }

            @Override
            public void onMessageSent(Message<?> message)
            {
                onSend = true;
            }

            @Override
            public void onResponseReceived()
            {
                onReceive = true;
            }

            @Override
            public void onResponseTimeout()
            {
                onTimeout = true;
            }

            @Override
            public double getBackPressureRateLimit()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public InetAddressAndPort getHost()
            {
                return host;
            }
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(Message msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(Message msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    /**
     * Make sure that if internode authenticatino fails for an outbound connection that all the code that relies
     * on getting the connection pool handles the null return
     *
     * @throws Exception
     */
    @Test
    public void testFailedInternodeAuth() throws Exception
    {
        MessagingService ms = MessagingService.instance();
        DatabaseDescriptor.setInternodeAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.250");

        //Should return null
        Message messageOut = Message.out(Verb.GOSSIP_DIGEST_ACK, emptyMessage);
        assertFalse(ms.isConnected(address, messageOut));

        //Should tolerate null
        ms.closeOutbound(address);
        ms.sendOneWay(messageOut, address);
    }

//    @Test
//    public void reconnectWithNewIp() throws Exception
//    {
//        InetAddressAndPort publicIp = InetAddressAndPort.getByName("127.0.0.2");
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.3");
//
//        // reset the preferred IP value, for good test hygene
//        SystemKeyspace.updatePreferredIP(publicIp, publicIp);
//
//        // create pool/conn with public addr
//        Assert.assertEquals(publicIp, messagingService.getCurrentEndpoint(publicIp));
//        messagingService.maybeReconnectWithNewIp(publicIp, privateIp).await(1L, TimeUnit.SECONDS);
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//
//        messagingService.closeOutbound(publicIp);
//
//        // recreate the pool/conn, and make sure the preferred ip addr is used
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//    }

    @Test
    public void testCloseInboundConnections() throws UnknownHostException, InterruptedException
    {
        try
        {
            messagingService.listen();
            Assert.assertTrue(messagingService.isListening());
            Assert.assertEquals(0, messagingService.inbound.connectionCount());

            // now, create a connection and make sure it's in a channel group
            InetAddressAndPort server = FBUtilities.getBroadcastAddressAndPort();

            CountDownLatch latch = new CountDownLatch(1);
            OutboundConnectionSettings settings = new OutboundConnectionSettings(server)
                                                  .withSocketSendBufferSizeInBytes(1 << 10)
                                                  .withDefaults(SMALL_MESSAGE, current_version);
//            Bootstrap bootstrap = OutboundConnectionInitiator.createBootstrap();
            AtomicReference<Channel> channel = new AtomicReference<>();
            OutboundConnectionInitiator.initiate(NettyFactory.instance.defaultGroup().next(), SMALL_MESSAGE, settings, current_version)
                                       .addListener(future -> {
                                           channel.set(((OutboundConnectionInitiator.Result)future.getNow()).success().channel);
                                                        latch.countDown();
                                       });
            latch.await(1, TimeUnit.SECONDS); // allow the netty pipeline/c* handshake to get set up
            Assert.assertNotNull(channel.get());

            int connectCount = messagingService.inbound.connectionCount();
            Assert.assertTrue(connectCount > 0);
        }
        finally
        {
            // last, shutdown the MS and make sure connections are removed
            messagingService.shutdown(true);
            int connectCount = messagingService.inbound.connectionCount();
            Assert.assertTrue(connectCount == 0);
        }
    }

    @Test
    public void listenPlainConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(false);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenPlainConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(false);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnectionWithLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddrAndLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenOptionalSecureConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(true);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenOptionalSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withEnabled(true)
                                                          .withOptional(true);
        listen(serverEncryptionOptions, true);
    }

    private void listen(ServerEncryptionOptions serverEncryptionOptions, boolean listenOnBroadcastAddr) throws InterruptedException
    {
        InetAddress listenAddress = null;
        if (listenOnBroadcastAddr)
        {
            DatabaseDescriptor.setShouldListenOnBroadcastAddress(true);
            listenAddress = InetAddresses.increment(FBUtilities.getBroadcastAddressAndPort().address);
            DatabaseDescriptor.setListenAddress(listenAddress);
            FBUtilities.reset();
        }

        InboundConnectionSettings settings = new InboundConnectionSettings()
                                             .withEncryption(serverEncryptionOptions);
        InboundConnections connections = new InboundConnections(settings);
        try
        {
            connections.open().await();
            Assert.assertTrue(connections.isOpen());

            Set<InetAddressAndPort> expect = new HashSet<>();
            expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getStoragePort()));
            if (settings.encryption.enable_legacy_ssl_storage_port)
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getSSLStoragePort()));
            if (listenOnBroadcastAddr)
            {
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().address, DatabaseDescriptor.getStoragePort()));
                if (settings.encryption.enable_legacy_ssl_storage_port)
                    expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().address, DatabaseDescriptor.getSSLStoragePort()));
            }

            Assert.assertEquals(expect.size(), connections.connectionCount());

            final int legacySslPort = DatabaseDescriptor.getSSLStoragePort();
            for (InboundConnections.InboundSocket socket : connections.sockets())
            {
                Assert.assertEquals(serverEncryptionOptions.enabled, socket.settings.encryption.enabled);
                Assert.assertEquals(serverEncryptionOptions.optional, socket.settings.encryption.optional);
                if (!serverEncryptionOptions.enabled)
                    Assert.assertFalse(legacySslPort == socket.settings.bindAddress.port);
                if (legacySslPort == socket.settings.bindAddress.port)
                    Assert.assertFalse(socket.settings.encryption.optional);
                Assert.assertTrue(socket.settings.bindAddress.toString(), expect.remove(socket.settings.bindAddress));
            }
        }
        finally
        {
            connections.close().await();
            Assert.assertEquals(0, connections.connectionCount());
        }
    }


//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.151", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.115", 7000);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(preferredIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIpOverridesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort local = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.4", 7000);
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.105", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }
}

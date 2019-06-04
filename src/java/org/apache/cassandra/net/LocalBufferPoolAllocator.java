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

import java.nio.ByteBuffer;

import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * Equivalent to {@link GlobalBufferPoolAllocator}, except explicitly using a specified
 * {@link org.apache.cassandra.utils.memory.BufferPool.LocalPool} to allocate from.
 *
 * Exists to facilitate more efficient handling large messages on the inbound path,
 * used by {@link ConnectionType#LARGE_MESSAGES} connections.
 *
 * TODO: enforce that get() and getAtLeast() are invoked from within the expected thread
 */
public class LocalBufferPoolAllocator extends BufferPoolAllocator
{
    private final BufferPool.LocalPool pool;

    LocalBufferPoolAllocator()
    {
        super();
        this.pool = new BufferPool.LocalPool();
    }

    @Override
    ByteBuffer get(int size)
    {
        return pool.take(size, false);
    }

    @Override
    ByteBuffer getAtLeast(int size)
    {
        return pool.take(size, true);
    }

    @Override
    public void release()
    {
        pool.release();
    }
}

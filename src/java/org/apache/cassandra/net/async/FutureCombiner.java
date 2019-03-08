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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

/**
 * Netty's PromiseCombiner is not threadsafe, and we combine futures from multiple event executors.
 *
 * This class groups a number of Future into a single logical Future, by registering a listener to each that
 * decrements a shared counter; if any of them fail, the FutureCombiner is completed immediately with the cause,
 * otherwise it will complete when the counter reaches zero.
 *
 * This Future is always uncancellable.
 *
 * We extend FutureDelegate, and simply provide it an uncancellable Promise that will be completed by the listeners
 * registered to the input futures.
 */
public class FutureCombiner extends FutureDelegate<Void>
{
    public FutureCombiner(Collection<? extends Future<?>> combine)
    {
        this(AsyncPromise.uncancellable(GlobalEventExecutor.INSTANCE), combine);
    }

    private FutureCombiner(Promise<Void> combined, Collection<? extends Future<?>> combine)
    {
        super(combined);

        AtomicInteger waitingOn = new AtomicInteger(combine.size());
        if (0 == waitingOn.get())
            combined.trySuccess(null);

        GenericFutureListener<? extends Future<Object>> listener = result -> {
            if (!result.isSuccess())
                combined.tryFailure(result.cause());
            else if (0 == waitingOn.decrementAndGet())
                combined.trySuccess(null);
        };

        for (Future<?> future : combine)
            future.addListener(listener);
    }

}

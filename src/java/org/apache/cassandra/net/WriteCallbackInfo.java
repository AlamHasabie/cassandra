/*
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
 */
package org.apache.cassandra.net;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;

public class WriteCallbackInfo extends CallbackInfo
{
    // either a Mutation, or a Paxos Commit (MessageOut)
    private final Object mutation;
    private final Replica replica;

    public WriteCallbackInfo(Replica replica,
                             IAsyncCallbackWithFailure<?> callback,
                             Message message,
                             ConsistencyLevel consistencyLevel,
                             boolean allowHints,
                             Verb verb)
    {
        super(replica.endpoint(), callback, verb);
        assert message != null;
        this.mutation = shouldHint(allowHints, message, consistencyLevel);
        //Local writes shouldn't go through messaging service (https://issues.apache.org/jira/browse/CASSANDRA-10477)
        assert (!target.equals(FBUtilities.getBroadcastAddressAndPort()));
        this.replica = replica;
    }

    public boolean shouldHint()
    {
        return mutation != null && StorageProxy.shouldHint(replica);
    }

    public Replica getReplica()
    {
        return replica;
    }

    public Mutation mutation()
    {
        return getMutation(mutation);
    }

    private static Mutation getMutation(Object object)
    {
        assert object instanceof Commit || object instanceof Mutation : object;
        return object instanceof Commit ? ((Commit) object).makeMutation()
                                        : (Mutation) object;
    }

    private static Object shouldHint(boolean allowHints, Message sentMessage, ConsistencyLevel consistencyLevel)
    {
        return allowHints
               && sentMessage.verb() != Verb.COUNTER_MUTATION_REQ
               && consistencyLevel != ConsistencyLevel.ANY
               ? sentMessage.payload : null;
    }

}

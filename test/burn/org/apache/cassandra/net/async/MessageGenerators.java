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

final class MessageGenerators
{
    final MessageGenerator small;
    final MessageGenerator large;

    MessageGenerators(MessageGenerator small, MessageGenerator large)
    {
        this.small = small;
        this.large = large;
    }

    MessageGenerator get(ConnectionType type)
    {
        switch (type)
        {
            case SMALL_MESSAGES:
            case URGENT_MESSAGES:
                return small;
            case LARGE_MESSAGES:
                return large;
            default:
                throw new IllegalStateException();
        }
    }
}
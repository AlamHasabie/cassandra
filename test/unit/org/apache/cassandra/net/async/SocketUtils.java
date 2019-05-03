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
import java.net.ServerSocket;

import com.google.common.base.Throwables;

public class SocketUtils
{
    public static synchronized int findAvailablePort() throws RuntimeException
    {
        ServerSocket ss = null;
        try
        {
            // let the system pick an ephemeral port
            ss = new ServerSocket(0);
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
        catch (IOException e)
        {
            throw Throwables.propagate(e);
        }
        finally
        {
            if (ss != null)
            {
                try
                {
                    ss.close();
                }
                catch (IOException e)
                {
                    Throwables.propagate(e);
                }
            }
        }
    }
}
/**
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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ShortNodeId;
import org.apache.cassandra.utils.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterMutationVerbHandler implements IVerbHandler
{
    private static Logger logger = LoggerFactory.getLogger(CounterMutationVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        byte[] bytes = message.getMessageBody();
        FastByteArrayInputStream buffer = new FastByteArrayInputStream(bytes);

        try
        {
            DataInputStream is = new DataInputStream(buffer);
            CounterMutation cm = CounterMutation.serializer().deserialize(is, message.getVersion());
            if (logger.isDebugEnabled())
              logger.debug("Applying forwarded " + cm);

            //Check Dependencies
            assert VersionUtil.extractDatacenter(cm.extractTimestamp()) != ShortNodeId.getLocalDC() : "Do not expect replication mutations from the localDC (yet)";
            if (cm.getDependencies().size() > 0) {
                StorageProxy.checkDependencies(cm.getTable(), cm.key(), cm.extractTimestamp(), cm.getDependencies(), new CounterMutationCompletion(message, id, cm));
            } else {
                applyAndRespond(message, id, cm);
            }
        }
        catch (IOException e)
        {
            logger.error("Error in counter mutation", e);
        }
    }

    protected void applyAndRespond(Message message, String id, CounterMutation cm)
    {
        try
        {
            String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            StorageProxy.applyCounterMutationOnLeader(cm, localDataCenter).get();
            WriteResponse response = new WriteResponse(cm.getTable(), cm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            MessagingService.instance().sendReply(responseMessage, id, message.getFrom());
        }
        catch (UnavailableException e)
        {
            // We check for UnavailableException in the coordinator not. It is
            // hence reasonable to let the coordinator timeout in the very
            // unlikely case we arrive here
        }
        catch (TimeoutException e)
        {
            // The coordinator node will have timeout itself so we let that goes
        }
        catch (IOException e)
        {
            logger.error("Error in counter mutation", e);
        }
    }

    private static class CMVHHandle
    {
        public static final CounterMutationVerbHandler instance = new CounterMutationVerbHandler();
    }

    public static CounterMutationVerbHandler instance()
    {
        return CMVHHandle.instance;
    }
}

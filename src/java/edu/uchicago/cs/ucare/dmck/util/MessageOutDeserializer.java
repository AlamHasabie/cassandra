package edu.uchicago.cs.ucare.dmck.util;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;

/**
* Helper class to deserialize message from an DataInput buffer
* Created since Cassandra does not implement MesssageOut deserialization
* The implementation, however, should be familiar with
* {@link org.apache.cassandra.net.MessageIn.read}, but return MessageOut instead
*/

public class MessageOutDeserializer 
{
	public static <T> MessageOut<T> read(DataInputPlus in, int version) throws IOException
	{
		InetAddress from = CompactEndpointSerializationHelper.deserialize(in);

        MessagingService.Verb verb = MessagingService.Verb.values()[in.readInt()];
        int parameterCount = in.readInt();
        Map<String, byte[]> parameters;
        if (parameterCount == 0)
        {
            parameters = Collections.emptyMap();
        }
        else
        {
            ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                builder.put(key, value);
            }
            parameters = builder.build();
        }

        int payloadSize = in.readInt();
        
        IVersionedSerializer<T> serializer = (IVersionedSerializer<T>) MessagingService.verbSerializers.get(verb);

        /** This part use a constructor which is annotated with @VisibleForTesting */
        if (payloadSize == 0 || serializer == null)
            return new MessageOut(from, verb, null, null, parameters);

        T payload = serializer.deserialize(in, version);
        return new MessageOut(from, verb, payload, serializer, parameters);
	}
}
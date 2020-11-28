package edu.uchicago.cs.ucare.dmck.filters;

import java.net.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.net.MessageOut;


public class InterceptorReadFilter implements IInterceptorVerbFilter<ReadCommand>
{
	private static final Logger logger = LoggerFactory.getLogger(InterceptorReadFilter.class);

	public boolean shouldIntercept(MessageOut<ReadCommand> m, int id, InetAddress to)
	{
		if((m.payload.metadata().ksName.equals("test"))&&(m.payload.metadata().cfName.equals("test"))){
			logger.trace("Bug message captured");
			return true;
		}

		logger.trace("Get read command for {}.{}, bypass", m.payload.metadata().ksName, m.payload.metadata().cfName);
		return false;
	}
}

package edu.uchicago.cs.ucare.dmck.filters;

import java.net.*;
import org.apache.cassandra.net.MessageOut;

public interface IInterceptorVerbFilter <T>
{
	/** Whether the interceptor should intercept*/
	public boolean shouldIntercept(MessageOut<T> m, int id, InetAddress to);
}

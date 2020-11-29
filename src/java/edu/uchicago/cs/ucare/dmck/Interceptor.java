package edu.uchicago.cs.ucare.dmck;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.config.DatabaseDescriptor;

import edu.uchicago.cs.ucare.dmck.filters.*;
import edu.uchicago.cs.ucare.dmck.util.*;

/** Interceptor class
* uses files for IPC
*/

public class Interceptor implements IMessageSink
{    
    private static final Logger logger = LoggerFactory.getLogger(Interceptor.class);

    public static final Interceptor instance;

    /* Lookup table for registering filters based on the verb. */
    private final Map<MessagingService.Verb, IInterceptorVerbFilter> verbFilters;
    public volatile Set<String> syncFilenameSet;

    /** Due to way message sending is handled */
    /** We need to keep track which message are already intercepted*/
    private volatile Set<Integer> interceptedMessage;

    /** @TODO : loading from an external conf file*/
    private final String ipcDir;
    private final WatcherThread watcher;
    private volatile boolean isWatching;

    static {
        instance = new Interceptor();
        instance.startWatcher();
        MessagingService.instance().addMessageSink(instance);
    }

    public Interceptor()
    {
        verbFilters = new EnumMap<>(MessagingService.Verb.class);

        registerVerbFilter(MessagingService.Verb.READ, new InterceptorReadFilter());

        syncFilenameSet = Collections.synchronizedSet(new HashSet<>());
        interceptedMessage = Collections.synchronizedSet(new HashSet<>());
        
        ipcDir = "/home/alam/cass-ipc";
        Path ackDir = Paths.get(ipcDir, "ack");
        isWatching = DatabaseDescriptor.getAllowInterceptor();
        watcher = new WatcherThread(ackDir);
    }

    public void turnOff(Exception e)
    {
        logger.error("Turning off watcher due to exception : ", e);
        turnOff();
    }
    public void turnOff()
    {
        isWatching = false;
    }

    private void registerVerbFilter(MessagingService.Verb verb, IInterceptorVerbFilter verbFilter)
    {
        assert !verbFilters.containsKey(verb);
        verbFilters.put(verb, verbFilter);
    }

    public static Interceptor instance()
    {
        return instance;
    }

    public boolean allowIncomingMessage(MessageIn m, int id)
    {
        return true;
    }

    public boolean allowOutgoingMessage(MessageOut m, int id, InetAddress to)
    {
        if(!isWatching)
        {
            return true;
        }

        if(!verbFilters.containsKey(m.verb))
        {
            return true;
        }

        if(interceptedMessage.contains(id))
        {
            return true;
        }

        if(!verbFilters.containsKey(m.verb))
        {
            return true;
        }

        if(!verbFilters.get(m.verb).shouldIntercept(m, id, to))
        {
            return true;
        }

        try {
            write(m, id, to);
            logger.trace("Intercepted message {}@{}", id, to);
            return false;
        } catch (IOException e)
        {
            logger.error("IOException ", e);
            return true;
        } catch (Exception e) 
        {
            logger.error("Exception occurs : ", e);
            return true;
        }
    }

    private int createHash(MessageOut m, int id, InetAddress to)
    {
        return m.from.hashCode() + Integer.hashCode(id);

    }
    private void write(MessageOut m, int id, InetAddress to) throws IOException
    {
        String filename = Integer.toString(createHash(m, id, to));

        syncFilenameSet.add(filename);
        File tmpFile = Paths.get(ipcDir.toString(), "new", filename + ".tmp")
                            .toFile();
        File mFile = Paths.get(ipcDir.toString(), "new", filename).toFile();

        tmpFile.createNewFile();
        FileOutputStream fs = new FileOutputStream(tmpFile, false);
        BufferedDataOutputStreamPlus out = new BufferedDataOutputStreamPlus(fs);

        out.writeInt(id);
        CompactEndpointSerializationHelper.serialize(to, out);
        m.serialize(out, MessagingService.current_version);

        out.flush();
        tmpFile.renameTo(mFile);
        logger.trace("successfully create message file : {}", filename);
    }

    private void startWatcher()
    {
        if(isWatching)
        {        
            Thread thread = new Thread(watcher);
            thread.start();
        }
    }

    private class WatcherThread implements Runnable 
    {
        private final Logger logger = LoggerFactory.getLogger(WatcherThread.class);
        private final Path ipcDir;

        public WatcherThread(Path path)
        {
            ipcDir = path;
        }

        private DataInputBuffer readFile(String filename) throws IOException
        {
            Path filepath = Paths.get(ipcDir.toString(), filename);  
            FileInputStream fs = new FileInputStream(filepath.toString());


            byte[] fileContent = new byte[(int) Files.size(filepath)];

            fs.read(fileContent);
            return new DataInputBuffer(Files.readAllBytes(filepath));
        }

        private void handleEvent(WatchEvent event)
        {
            try {
                String filename = event.context().toString();
                logger.trace("Set size {}", Interceptor.instance().syncFilenameSet.size());

                if(Interceptor.instance().syncFilenameSet.contains(filename)) {
                    DataInputBuffer inputBuffer = readFile(filename); 

                    logger.trace("Read {} bytes from {}", inputBuffer.available(), filename);

                    int id = inputBuffer.readInt();
                    InetAddress to = CompactEndpointSerializationHelper.deserialize(inputBuffer);

                    MessageOut message = MessageOutDeserializer.read(
                        inputBuffer, 
                        MessagingService.current_version);

                    logger.trace("Finished message deserialization {}:{}", id, to);
                    Interceptor.instance().syncFilenameSet.remove(filename);
                    Interceptor.instance().interceptedMessage.add(id);
                    MessagingService.instance().sendOneWay(message, id, to);
                }
            }
            catch (IOException e)
            {
                logger.error("IOException occurs during event handling", e);
            }
            catch (Exception e)
            {
                logger.error("Exception occurs during event handling", e);
            }
        }

        public void run()
        {
            try 
            {
                logger.info("Starting intercepted message watching thread...");
                WatchService watchService = FileSystems.getDefault().newWatchService();

                ipcDir.register(
                    watchService, 
                    StandardWatchEventKinds.ENTRY_CREATE
                );

                WatchKey key;
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if(event.kind().equals(StandardWatchEventKinds.OVERFLOW))
                            continue;

                        if(event.kind().equals(StandardWatchEventKinds.ENTRY_CREATE)) {
                            logger.trace("Message {}:{} captured", event.kind(), event.context());
                            handleEvent((WatchEvent) event);
                        }
                    }
                key.reset();
                }
            } 
            catch (IOException e)
            {
                logger.error("Exception occurs in WatcherThread", e);
                Interceptor.instance().turnOff(e);
            }
            catch (Exception e)
            {
                logger.error("Exception", e);
                Interceptor.instance().turnOff(e);
            }
            finally {
                return;
            }
        }
    }
}

package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import it.sauronsoftware.ftp4j.FTPAbortedException;
import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPDataTransferException;
import it.sauronsoftware.ftp4j.FTPException;
import it.sauronsoftware.ftp4j.FTPIllegalReplyException;
import it.sauronsoftware.ftp4j.FTPListParseException;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.FtpFileInputPlugin.PluginTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FtpFileInputPlugin.class)
public class TestFtpFileInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Logger log;
    private ConfigSource config;
    private FtpFileInputPlugin plugin;
    private FTPClient client;

    @Before
    public void createResources()
    {
        log = runtime.getExec().getLogger(TestFtpFileInputPlugin.class);
        config = runtime.getExec().newConfigSource()
                .set("type", "ftp")
                .set("host", "my_host")
                .set("path_prefix", "my_path_prefix");
        plugin = spy(runtime.getInstance(FtpFileInputPlugin.class));
        client = spy(new FTPClient());
    }

    @Test
    public void cleanup()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happen
    }

    @Test
    public void disconnectClient()
            throws Exception
    {
        { // if isConnected returns false, it does nothing
            doReturn(false).when(client).isConnected();
            FtpFileInputPlugin.disconnectClient(client); // no errors happen
        }

        doReturn(true).when(client).isConnected();
        // if FTPClient#disconnect throws FTP, FTPIllegalReply or IOException, no errors happen.

        {
            doThrow(new FTPException(0)).when(client).disconnect(anyBoolean());
            FtpFileInputPlugin.disconnectClient(client); // no errors happen
        }

        {
            doThrow(new FTPIllegalReplyException()).when(client).disconnect(anyBoolean());
            FtpFileInputPlugin.disconnectClient(client); // no errors happen
        }

        {
            doThrow(new IOException("")).when(client).disconnect(anyBoolean());
            FtpFileInputPlugin.disconnectClient(client); // no errors happen
        }
    }

    @Test
    public void listFilesByPrefix()
            throws Exception
    {
        doNothing().when(client).changeDirectory(anyString());
        doReturn("/").when(client).currentDirectory();

        // if FTPClient#list throws Exception

        { // FTPListParse
            doThrow(new FTPListParseException()).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof FTPListParseException);
            }
        }
        { // FTPAborted
            doThrow(new FTPAbortedException()).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof FTPAbortedException);
            }
        }
        { // FTPDataTransfer
            doThrow(new FTPDataTransferException()).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof FTPDataTransferException);
            }
        }
        { // FTP
            doThrow(new FTPException(0)).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof FTPException);
            }
        }
        { // FTPIllegalReply
            doThrow(new FTPIllegalReplyException()).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof FTPIllegalReplyException);
            }
        }
        { // IOException
            doThrow(new IOException("")).when(client).list();
            try {
                FtpFileInputPlugin.listFilesByPrefix(log, client, "/", Optional.<String>absent());
                fail();
            }
            catch (Throwable t) {
                assertTrue(t.getCause() instanceof IOException);
            }
        }
    }

}

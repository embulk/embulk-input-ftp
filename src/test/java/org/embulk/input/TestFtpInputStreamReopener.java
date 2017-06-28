package org.embulk.input;

import it.sauronsoftware.ftp4j.FTPClient;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.FtpFileInputPlugin.FtpInputStreamReopener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FtpFileInputPlugin.class)
public class TestFtpInputStreamReopener
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private FTPClient client;
    private Logger log;
    private ExecutorService executor;

    @Before
    public void createResources()
    {
        client = mock(FTPClient.class);
        log = runtime.getExec().getLogger(TestFtpInputStreamReopener.class);
        executor = Executors.newCachedThreadPool();
    }

    @After
    public void destroyResources()
    {
        executor.shutdown();
    }

    @Test
    public void reopenFileByReopener()
            throws Exception
    {
        PowerMockito.mockStatic(FtpFileInputPlugin.class);

        { // not retry
            when(FtpFileInputPlugin.startDownload(any(Logger.class), any(FTPClient.class), anyString(),
                    anyLong(), any(ExecutorService.class))).thenReturn(new ByteArrayInputStream("value".getBytes()));

            FtpInputStreamReopener opener = new FtpInputStreamReopener(log, client, executor, "/");
            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
            }
        }

        { // retry once
            when(FtpFileInputPlugin.startDownload(any(Logger.class), any(FTPClient.class), anyString(),
                    anyLong(), any(ExecutorService.class))).thenThrow(new RuntimeException()).thenReturn(new ByteArrayInputStream("value".getBytes()));

            FtpInputStreamReopener opener = new FtpInputStreamReopener(log, client, executor, "/");
            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                assertEquals("value", r.readLine());
            }
        }

        { // retry count exceeded the threshold
            FtpFileInputPlugin.RETRY_LIMIT = 0;
            when(FtpFileInputPlugin.startDownload(any(Logger.class), any(FTPClient.class), anyString(),
                    anyLong(), any(ExecutorService.class))).thenThrow(new RuntimeException()).thenThrow(new RuntimeException());

            FtpInputStreamReopener opener = new FtpInputStreamReopener(log, client, executor, "/");
            try (InputStream in = opener.reopen(0, new RuntimeException())) {
                fail();
            }
            catch (Throwable t) {
            }
        }
    }
}

package org.embulk.util.ftp;

import com.google.common.io.Resources;
import org.embulk.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.security.cert.X509Certificate;
import java.util.List;

public class TestTrustManagers
{
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_DATA;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResources()
    {
        FTP_TEST_SSL_TRUSTED_CA_CERT_DATA = Resources.getResource("ftp.crt").getPath();
    }

    @Test
    public void testNewTrustManager() throws Exception
    {
        StringReader r = new StringReader(FTP_TEST_SSL_TRUSTED_CA_CERT_DATA);
        List<X509Certificate> certs = TrustManagers.readPemEncodedX509Certificates(r);
        TrustManagers.newTrustManager(certs); // no error happens
    }

    @Test
    public void testNewDefaultJavaTrustManager() throws Exception
    {
        TrustManagers.newDefaultJavaTrustManager(); // no error happens
    }

    @Test
    public void testNewSSLSocketFactory() throws Exception
    {
        TrustManagers.newSSLSocketFactory(null, null, "example.com"); // no error happens
    }
}

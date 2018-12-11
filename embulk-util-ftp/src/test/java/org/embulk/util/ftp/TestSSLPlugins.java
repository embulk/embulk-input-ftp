package org.embulk.util.ftp;

import com.google.common.io.Resources;
import org.embulk.EmbulkTestRuntime;
import org.embulk.util.ftp.SSLPlugins.SSLPluginConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestSSLPlugins
{
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_FILE;
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_DATA;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResources() throws Exception
    {
        FTP_TEST_SSL_TRUSTED_CA_CERT_FILE = Resources.getResource("ftp.crt").getPath();
        FTP_TEST_SSL_TRUSTED_CA_CERT_DATA = getFileContents(Resources.getResource("ftp.crt").getPath());
    }

    @Test
    public void testNewTrustManager() throws Exception
    {
        StringReader r = new StringReader(FTP_TEST_SSL_TRUSTED_CA_CERT_DATA);
        List<X509Certificate> certs = TrustManagers.readPemEncodedX509Certificates(r);
        SSLPluginConfig config = new SSLPluginConfig(certs, false);
        config.newTrustManager(); // no error happens
    }

    @Test
    public void testNewSSLSocketFactory() throws Exception
    {
        StringReader r = new StringReader(FTP_TEST_SSL_TRUSTED_CA_CERT_DATA);
        List<X509Certificate> certs = TrustManagers.readPemEncodedX509Certificates(r);
        SSLPluginConfig config = new SSLPluginConfig(certs, false);

        SSLPlugins.newSSLSocketFactory(config, "example.com"); // no error happens
    }

    @Test
    public void testSslPluginConfigure()
    {
        PluginTask task = new PluginTask();
        task.setSslTrustedCaCertFile(FTP_TEST_SSL_TRUSTED_CA_CERT_FILE);
        SSLPlugins.configure(task); // no error happens
    }

    @Test
    public void testSslPluginConfigureWithVerify()
    {
        PluginTask task = new PluginTask();
        task.setSslVerify(true);
        task.setSslTrustedCaCertFile(FTP_TEST_SSL_TRUSTED_CA_CERT_FILE);
        SSLPlugins.configure(task); // no error happens
    }

    @Test
    public void testReadTrustedCertificatesWithFile()
    {
        PluginTask task = new PluginTask();
        task.setSslTrustedCaCertFile(FTP_TEST_SSL_TRUSTED_CA_CERT_FILE);

        Optional<List<X509Certificate>> certs = SSLPlugins.readTrustedCertificates(task);
        assertThat(certs.get().size(), is(1));
        X509Certificate cert = certs.get().get(0);
        assertThat(cert.getSerialNumber().toString(), is("17761656583521120896"));
        assertThat(cert.getIssuerDN().toString(), is("CN=example.com, OU=Development devision, O=nobody.inc, L=Chiyoda-ku, ST=Tokyo, C=JP"));
    }

    @Test
    public void testReadTrustedCertificatesWithData()
    {
        PluginTask task = new PluginTask();
        task.setSslTrustedCaCertData(FTP_TEST_SSL_TRUSTED_CA_CERT_DATA);

        Optional<List<X509Certificate>> certs = SSLPlugins.readTrustedCertificates(task);
        assertThat(certs.get().size(), is(1));
        X509Certificate cert = certs.get().get(0);
        assertThat(cert.getSerialNumber().toString(), is("17761656583521120896"));
        assertThat(cert.getIssuerDN().toString(), is("CN=example.com, OU=Development devision, O=nobody.inc, L=Chiyoda-ku, ST=Tokyo, C=JP"));
    }

    private class PluginTask implements SSLPlugins.SSLPluginTask
    {
        private Optional<Boolean> sslVerify = Optional.of(false);
        private Optional<String> caCertData = Optional.empty();
        private Optional<String> caCertFile = Optional.empty();
        @Override
        public boolean getSslVerifyHostname()
        {
            return false;
        }

        @Override
        public Optional<Boolean> getSslVerify()
        {
            return this.sslVerify;
        }

        public void setSslVerify(Boolean sslVerify)
        {
            this.sslVerify = Optional.of(sslVerify);
        }

        @Override
        public Optional<String> getSslTrustedCaCertFile()
        {
            return this.caCertFile;
        }

        public void setSslTrustedCaCertFile(String certFile)
        {
            this.caCertFile = Optional.of(certFile);
        }

        @Override
        public Optional<String> getSslTrustedCaCertData()
        {
            return this.caCertData;
        }

        public void setSslTrustedCaCertData(String certData)
        {
            this.caCertData = Optional.of(certData);
        }
    }

    private String getFileContents(String path) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = new FileInputStream(new File(path))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();

            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
        }
        return sb.toString();
    }
}

package org.embulk.input.ftp;

import java.util.List;
import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.SecureRandom;
import java.security.KeyManagementException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import sun.security.validator.KeyStores;
import org.bouncycastle.openssl.PEMParser;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;

public class SSLPlugins {
    // SSLPlugins is only for SSL clients. SSL server implementation is out ouf scope.

    public interface SSLPluginTask
    {
        @Config("ssl_no_verify")
        @ConfigDefault("null")
        public Optional<Boolean> getSslNoVerify();

        @Config("ssl_trusted_ca_cert_file")
        @ConfigDefault("null")
        public Optional<String> getSslTrustedCaCertFile();

        @Config("ssl_trusted_ca_cert_data")
        @ConfigDefault("null")
        public Optional<String> getSslTrustedCaCertData();
    }

    private static class NoVerifyTrustManager
            implements X509TrustManager
    {
        static final NoVerifyTrustManager INSTANCE = new NoVerifyTrustManager();

        private NoVerifyTrustManager()
        { }

        public X509Certificate[] getAcceptedIssuers()
        {
            return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType)
        { }

        public void checkServerTrusted(X509Certificate[] certs, String authType)
        { }
    }

    private static class VerifyServerCertificateIssuersTrustManager
            implements X509TrustManager
    {
        private final X509Certificate[] trustedCertificates;

        public VerifyServerCertificateIssuersTrustManager(List<X509Certificate> trustedCertificates)
        {
            this.trustedCertificates = trustedCertificates.toArray(new X509Certificate[trustedCertificates.size()]);
        }

        public X509Certificate[] getAcceptedIssuers()
        {
            return trustedCertificates;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType)
        { }

        public void checkServerTrusted(X509Certificate[] certs, String authType)
        { }
    }

    private static X509TrustManager getNoVerifyTrustManager()
    {
        return NoVerifyTrustManager.INSTANCE;
    }

    private static X509TrustManager newVerifyServerCertificateIssuersTrustManager(List<X509Certificate> trustedCertificates)
    {
        return new VerifyServerCertificateIssuersTrustManager(trustedCertificates);
    }

    private static List<X509Certificate> getJvmTrustedCaCerts()
    {
        String path = System.getProperty("org.openjdk.system.security.cacerts", System.getProperty("java.home")+"/lib/security/cacerts");
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            try (InputStream in = new FileInputStream(path)) {
                keyStore.load(in, null);
            }
            // TODO don't use sun.security.validator.KeyStores.
            return ImmutableList.copyOf(KeyStores.getTrustedCerts(keyStore));
        } catch (FileNotFoundException ex) {
            throw new RuntimeException("Can't find JVM root cacerts file at '" + path +"'. Please confirm that org.openjdk.system.security.cacerts system property is properly set.", ex);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read JVM root cacerts file '" + path +"'. Please confirm that org.openjdk.system.security.cacerts system property is properly set.", ex);
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Optional<List<X509Certificate>> readX509Certificates(SSLPluginTask task)
    {
        String optionName;
        Reader reader;
        if (task.getSslTrustedCaCertData().isPresent()) {
            optionName = "ssl_trusted_ca_cert_data";
            reader = new StringReader(task.getSslTrustedCaCertData().get());
        } else if (task.getSslTrustedCaCertFile().isPresent()) {
            optionName = "ssl_trusted_ca_cert_file '" + task.getSslTrustedCaCertFile().get() + "'";
            try {
                reader = new FileReader(task.getSslTrustedCaCertFile().get());
            } catch (IOException ex) {
                throw new ConfigException("Failed to open "+optionName, ex);
            }
        } else {
            return Optional.absent();
        }

        ImmutableList.Builder<X509Certificate> builder = ImmutableList.builder();

        try (PEMParser pemParser = new PEMParser(reader)) {
            while (true) {
                Object pem = pemParser.readObject();

                if (pem == null) {
                    break;
                }

                if (pem instanceof X509Certificate) {
                    builder.add((X509Certificate) pem);
                }
            }
        } catch (IOException ex) {
            throw new ConfigException("Failed to read " + optionName, ex);
        //} catch (PEMException ex) {
        //    // throw when parsing PemObject to Object fails
        //} catch (IOException ex) {
        //    if (ex.getClass().equals(IOException.class)) {
        //        String message = ex.getMessage();
        //        if (message.startsWith("unrecognised object: ")) {
        //            // thrown at org.bouncycastle.openssl.PemParser.readObject when key type (header of a pem) is
        //            // unknown.
        //        } else if (message.startsWith("-----END ") && message.endsWith(" not found")) {
        //            // thrown at org.bouncycastle.util.io.pem.PemReader.loadObject when a pem file format is invalid
        //        }
        //    } else {
        //        throw ex;
        //    }
        //}
        }

        List<X509Certificate> certs = builder.build();
        if (certs.isEmpty()) {
            throw new ConfigException(optionName + " does not include valid X.509 PEM certificates");
        }

        return Optional.of(certs);
    }

    public static enum DefaultVerifyMode
    {
        VERIFY_BY_JVM_TRUSTED_CA_CERTS,
        NO_VERIFY;
    };

    public static X509TrustManager newTrustManager(SSLPluginTask task)
    {
        return newTrustManager(task, DefaultVerifyMode.VERIFY_BY_JVM_TRUSTED_CA_CERTS);
    }

    public static X509TrustManager newTrustManager(SSLPluginTask task, DefaultVerifyMode defaultVerifyMode)
    {
        Optional<List<X509Certificate>> certs = readX509Certificates(task);
        if (certs.isPresent()) {
            return newVerifyServerCertificateIssuersTrustManager(certs.get());
        } else if (task.getSslNoVerify().isPresent()) {
            if (task.getSslNoVerify().get()) {
                return getNoVerifyTrustManager();
            } else {
                return newVerifyServerCertificateIssuersTrustManager(getJvmTrustedCaCerts());
            }
        } else {
            switch (defaultVerifyMode) {
            case VERIFY_BY_JVM_TRUSTED_CA_CERTS:
                return newVerifyServerCertificateIssuersTrustManager(getJvmTrustedCaCerts());
            case NO_VERIFY:
                return getNoVerifyTrustManager();
            default:
                throw new AssertionError();
            }
        }
    }

    public static SSLSocketFactory newSSLSocketFactory(SSLPluginTask task)
    {
        return newSSLSocketFactory(task, DefaultVerifyMode.VERIFY_BY_JVM_TRUSTED_CA_CERTS);
    }

    public static SSLSocketFactory newSSLSocketFactory(SSLPluginTask task, DefaultVerifyMode defaultVerifyMode)
    {
        TrustManager trustManager = newTrustManager(task, defaultVerifyMode);

        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(
                    null,  // TODO sending client certificate is not implemented yet
                    new TrustManager[] { trustManager },
                    new SecureRandom());
            return context.getSocketFactory();

        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);

        } catch (KeyManagementException ex) {
            throw new RuntimeException(ex);
        }
    }
}

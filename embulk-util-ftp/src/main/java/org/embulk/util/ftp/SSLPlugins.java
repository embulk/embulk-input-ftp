package org.embulk.util.ftp;

import java.util.List;
import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateEncodingException;
import java.security.KeyManagementException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;

public class SSLPlugins
{
    // SSLPlugins is only for SSL clients. SSL server implementation is out ouf scope.

    public interface SSLPluginTask
    {
        @Config("ssl_verify")
        @ConfigDefault("null")
        public Optional<Boolean> getSslVerify();

        @Config("ssl_verify_hostname")
        @ConfigDefault("true")
        public boolean getSslVerifyHostname();

        @Config("ssl_trusted_ca_cert_file")
        @ConfigDefault("null")
        public Optional<String> getSslTrustedCaCertFile();

        @Config("ssl_trusted_ca_cert_data")
        @ConfigDefault("null")
        public Optional<String> getSslTrustedCaCertData();
    }

    private static enum VerifyMode
    {
        NO_VERIFY,
        CERTIFICATES,
        JVM_DEFAULT;
    }

    public static class SSLPluginConfig
    {
        static SSLPluginConfig NO_VERIFY = new SSLPluginConfig(VerifyMode.NO_VERIFY, false, ImmutableList.<byte[]>of());

        private final VerifyMode verifyMode;
        private final boolean verifyHostname;
        private final List<X509Certificate> certificates;

        @JsonCreator
        private SSLPluginConfig(
            @JsonProperty("verifyMode") VerifyMode verifyMode,
            @JsonProperty("verifyHostname") boolean verifyHostname,
            @JsonProperty("certificates") List<byte[]> certificates)
        {
            this.verifyMode = verifyMode;
            this.verifyHostname = verifyHostname;
            this.certificates = ImmutableList.copyOf(
                    Lists.transform(certificates, new Function<byte[], X509Certificate>() {
                        public X509Certificate apply(byte[] data)
                        {
                            try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
                                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                                return (X509Certificate) cf.generateCertificate(in);
                            } catch (IOException | CertificateException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    })
                );
        }

        SSLPluginConfig(List<X509Certificate> certificates, boolean verifyHostname)
        {
            this.verifyMode = VerifyMode.CERTIFICATES;
            this.verifyHostname = verifyHostname;
            this.certificates = certificates;
        }

        static SSLPluginConfig useJvmDefault(boolean verifyHostname)
        {
            return new SSLPluginConfig(VerifyMode.JVM_DEFAULT, verifyHostname, ImmutableList.<byte[]>of());
        }

        @JsonProperty("verifyMode")
        private VerifyMode getVerifyMode()
        {
            return verifyMode;
        }

        @JsonProperty("verifyHostname")
        private boolean getVerifyHostname()
        {
            return verifyHostname;
        }

        @JsonProperty("certificates")
        private List<byte[]> getCertData()
        {
            return Lists.transform(certificates, new Function<X509Certificate, byte[]>() {
                public byte[] apply(X509Certificate cert)
                {
                    try {
                        return cert.getEncoded();
                    } catch (CertificateEncodingException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }

        @JsonIgnore
        public X509TrustManager[] newTrustManager()
        {
            try {
                switch (verifyMode) {
                case NO_VERIFY:
                    return new X509TrustManager[] { getNoVerifyTrustManager() };
                case CERTIFICATES:
                    return TrustManagers.newTrustManager(certificates);
                default: // JVM_DEFAULT
                    return TrustManagers.newDefaultJavaTrustManager();
                }
            } catch (IOException | GeneralSecurityException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static enum DefaultVerifyMode
    {
        VERIFY_BY_JVM_TRUSTED_CA_CERTS,
        NO_VERIFY;
    };

    public static SSLPluginConfig configure(SSLPluginTask task)
    {
        return configure(task, DefaultVerifyMode.VERIFY_BY_JVM_TRUSTED_CA_CERTS);
    }

    public static SSLPluginConfig configure(SSLPluginTask task, DefaultVerifyMode defaultVerifyMode)
    {
        boolean verify = task.getSslVerify().or(defaultVerifyMode != DefaultVerifyMode.NO_VERIFY);
        if (verify) {
            Optional<List<X509Certificate>> certs = readTrustedCertificates(task);
            if (certs.isPresent()) {
                return new SSLPluginConfig(certs.get(), task.getSslVerifyHostname());
            } else {
                return SSLPluginConfig.useJvmDefault(task.getSslVerifyHostname());
            }
        } else {
            return SSLPluginConfig.NO_VERIFY;
        }
    }

    private static Optional<List<X509Certificate>> readTrustedCertificates(SSLPluginTask task)
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

        List<X509Certificate> certs;
        try (Reader r = reader) {
            certs = TrustManagers.readPemEncodedX509Certificates(r);
            if (certs.isEmpty()) {
                throw new ConfigException(optionName + " does not include valid X.509 PEM certificates");
            }
        } catch (CertificateException | IOException ex) {
            throw new ConfigException("Failed to read "+optionName, ex);
        }

        return Optional.of(certs);
    }

    public static SSLSocketFactory newSSLSocketFactory(SSLPluginConfig config, String hostname)
    {
        try {
            return TrustManagers.newSSLSocketFactory(
                    null,  // TODO sending client certificate is not implemented yet
                    config.newTrustManager(),
                    config.getVerifyHostname() ? hostname : null);
        } catch (KeyManagementException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static class NoVerifyTrustManager
            implements X509TrustManager
    {
        static final NoVerifyTrustManager INSTANCE = new NoVerifyTrustManager();

        private NoVerifyTrustManager()
        { }

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType)
        { }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType)
        { }
    }

    private static X509TrustManager getNoVerifyTrustManager()
    {
        return NoVerifyTrustManager.INSTANCE;
    }
}

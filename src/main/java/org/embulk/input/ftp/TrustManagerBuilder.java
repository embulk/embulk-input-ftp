package org.embulk.input.ftp;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.Reader;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidAlgorithmParameterException;
import java.security.cert.TrustAnchor;
import java.security.cert.PKIXParameters;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;

public class TrustManagerBuilder
{
    public static KeyStore readDefaultJavaKeyStore() throws IOException, KeyStoreException, CertificateException
    {
        String path = (System.getProperty("java.home") + "/lib/security/cacerts").replace('/', File.separatorChar);
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            try (FileInputStream in = new FileInputStream(path)) {
                keyStore.load(in, null);  // password=null because cacerts file is not encrypted
            }
            return keyStore;
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);  // TODO assertion exception?
        }
    }

    public static List<X509Certificate> readDefaultJavaTrustedCertificates() throws IOException, CertificateException, KeyStoreException, InvalidAlgorithmParameterException
    {
        KeyStore keyStore = readDefaultJavaKeyStore();
        PKIXParameters params = new PKIXParameters(keyStore);
        List<X509Certificate> certs = new ArrayList<>();
        for (TrustAnchor trustAnchor : params.getTrustAnchors() ) {
            certs.add(trustAnchor.getTrustedCert());
        }
        return certs;
    }

    public static List<X509Certificate> readPemEncodedX509Certificates(Reader reader) throws IOException, CertificateException
    {
        // this method abuses CertificateParsingException because its javadoc says
        // CertificateParsingException is only for DER-encoded formats.

        JcaX509CertificateConverter conv = new JcaX509CertificateConverter();
        List<X509Certificate> certs = new ArrayList<>();

        try {
            PEMParser pemParser = new PEMParser(reader);
            // PEMParser#close is unnecessary because it just closes underlying reader

            while (true) {
                Object pem = pemParser.readObject();

                if (pem == null) {
                    break;
                }

                if (pem instanceof X509CertificateHolder) {
                    X509Certificate cert = conv.getCertificate((X509CertificateHolder) pem);
                    certs.add(cert);
                }
            }

        } catch (PEMException ex) {
            // throw when parsing PemObject to Object fails
            throw new CertificateParsingException(ex);

        } catch (IOException ex) {
            if (ex.getClass().equals(IOException.class)) {
                String message = ex.getMessage();
                if (message.startsWith("unrecognised object: ")) {
                    // thrown at org.bouncycastle.openssl.PemParser.readObject when key type (header of a pem) is
                    // unknown.
                    throw new CertificateParsingException(ex);
                } else if (message.startsWith("-----END ") && message.endsWith(" not found")) {
                    // thrown at org.bouncycastle.util.io.pem.PemReader.loadObject when a pem file format is invalid
                    throw new CertificateParsingException(ex);
                }
            } else {
                throw ex;
            }
        }

        return certs;
    }

    public static KeyStore buildKeyStoreFromTrustedCertificates(List<X509Certificate> certificates) throws KeyStoreException
    {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try {
            keyStore.load(null);
        } catch (IOException | CertificateException | NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
        int i = 0;
        for (X509Certificate cert : certificates) {
            keyStore.setCertificateEntry("cert_" + i, cert);
            i++;
        }
        return keyStore;
    }

    public static X509TrustManager[] newTrustManager(List<X509Certificate> trustedCertificates) throws KeyStoreException
    {
        try {
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore keyStore = buildKeyStoreFromTrustedCertificates(trustedCertificates);
            factory.init(keyStore);
            List<X509TrustManager> tms = new ArrayList<>();
            for (TrustManager tm : factory.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    tms.add((X509TrustManager) tm);
                }
            }
            return tms.toArray(new X509TrustManager[tms.size()]);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);  // TODO assertion exception?
        }
    }

    public static X509TrustManager[] newDefaultJavaTrustManager() throws IOException, CertificateException, KeyStoreException, InvalidAlgorithmParameterException
    {
        return newTrustManager(readDefaultJavaTrustedCertificates());
    }
}

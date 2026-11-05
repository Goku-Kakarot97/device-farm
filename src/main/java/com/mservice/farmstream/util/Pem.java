package com.mservice.farmstream.util;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** PEM certificate + key files (the self-signed pair in {@code ssl/}) into an {@link SSLContext}; trust-all client contexts. */
public final class Pem {

    private static final Pattern BLOCK = Pattern.compile("-----BEGIN ([A-Z ]+)-----([^-]+)-----END \\1-----", Pattern.DOTALL);

    private Pem() {
    }

    /** A server context from a PEM certificate (chain) and a PKCS#8 ("BEGIN PRIVATE KEY") or PKCS#1 RSA key. */
    public static SSLContext serverContext(Path certFile, Path keyFile) throws IOException, GeneralSecurityException {
        List<Certificate> chain = new ArrayList<>();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        String certPem = Files.readString(certFile, StandardCharsets.US_ASCII);
        Matcher m = BLOCK.matcher(certPem);
        while (m.find()) {
            if (m.group(1).equals("CERTIFICATE")) {
                chain.add(cf.generateCertificate(new ByteArrayInputStream(decode(m.group(2)))));
            }
        }
        if (chain.isEmpty()) {
            throw new IOException("no CERTIFICATE block in " + certFile);
        }
        PrivateKey key = readPrivateKey(Files.readString(keyFile, StandardCharsets.US_ASCII), keyFile);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        char[] pw = "farm".toCharArray();
        ks.setKeyEntry("server", key, pw, chain.toArray(new Certificate[0]));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, new SecureRandom());
        return ctx;
    }

    private static PrivateKey readPrivateKey(String pem, Path file) throws IOException, GeneralSecurityException {
        Matcher m = BLOCK.matcher(pem);
        while (m.find()) {
            String type = m.group(1);
            byte[] der = decode(m.group(2));
            if (type.equals("PRIVATE KEY")) {
                PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
                for (String alg : new String[] {"RSA", "EC", "Ed25519"}) {
                    try {
                        return KeyFactory.getInstance(alg).generatePrivate(spec);
                    } catch (GeneralSecurityException ignored) {
                        // try the next algorithm
                    }
                }
                throw new IOException("unsupported private key algorithm in " + file);
            }
            if (type.equals("RSA PRIVATE KEY")) {
                return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(pkcs1ToPkcs8(der)));
            }
        }
        throw new IOException("no PRIVATE KEY block in " + file);
    }

    /** Wraps a PKCS#1 RSAPrivateKey DER into a PKCS#8 PrivateKeyInfo (version 0, rsaEncryption OID, NULL params). */
    private static byte[] pkcs1ToPkcs8(byte[] pkcs1) {
        byte[] algId = {0x30, 0x0d, 0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00};
        byte[] version = {0x02, 0x01, 0x00};
        byte[] octet = derTlv(0x04, pkcs1);
        byte[] body = new byte[version.length + algId.length + octet.length];
        System.arraycopy(version, 0, body, 0, version.length);
        System.arraycopy(algId, 0, body, version.length, algId.length);
        System.arraycopy(octet, 0, body, version.length + algId.length, octet.length);
        return derTlv(0x30, body);
    }

    private static byte[] derTlv(int tag, byte[] content) {
        byte[] len;
        int n = content.length;
        if (n < 0x80) {
            len = new byte[] {(byte) n};
        } else if (n < 0x100) {
            len = new byte[] {(byte) 0x81, (byte) n};
        } else if (n < 0x10000) {
            len = new byte[] {(byte) 0x82, (byte) (n >> 8), (byte) n};
        } else {
            len = new byte[] {(byte) 0x83, (byte) (n >> 16), (byte) (n >> 8), (byte) n};
        }
        byte[] out = new byte[1 + len.length + n];
        out[0] = (byte) tag;
        System.arraycopy(len, 0, out, 1, len.length);
        System.arraycopy(content, 0, out, 1 + len.length, n);
        return out;
    }

    private static byte[] decode(String base64) {
        return Base64.getMimeDecoder().decode(base64.replaceAll("\\s", ""));
    }

    /**
     * A client context that accepts any server certificate and any host name (the farm and the relay use
     * self-signed certs). Must be an {@link X509ExtendedTrustManager}: with a plain {@link X509TrustManager}
     * JSSE wraps it and still performs host-name verification when the engine has the "HTTPS" endpoint
     * identification algorithm set (which hydrangea's TlsStage.client always does) and fails with
     * "No name matching ... found".
     */
    public static SSLContext trustAll() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] {new X509ExtendedTrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }}, new SecureRandom());
            return ctx;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A client engine that neither verifies the certificate nor the host name (like Node's {@code rejectUnauthorized: false}). */
    public static SSLEngine insecureClientEngine(String host, int port) {
        SSLEngine engine = trustAll().createSSLEngine(host, port);
        engine.setUseClientMode(true);
        return engine;
    }
}

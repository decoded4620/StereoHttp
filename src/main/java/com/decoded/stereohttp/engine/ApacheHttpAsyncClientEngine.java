package com.decoded.stereohttp.engine;

import com.decoded.stereohttp.*;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;


public class ApacheHttpAsyncClientEngine {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpAsyncClientEngine.class);
  private SSLContext sslContext;
  private CloseableHttpAsyncClient httpAsyncClient;
  private SSLIOSessionStrategy sslSessionStrategy;

  private String keystorePath = "/keystore.jks"; // or .p12
  private String keystorePass = "passwd";
  private String keyPass = "keypasswd";
  private KeystoreType keystoreType = KeystoreType.JKS;

  public ApacheHttpAsyncClientEngine() {

  }

  public ApacheHttpAsyncClientEngine setKeyPass(final String keyPass) {
    this.keyPass = keyPass;
    return this;
  }

  public ApacheHttpAsyncClientEngine setKeystorePass(final String keystorePass) {
    this.keystorePass = keystorePass;
    return this;
  }

  public ApacheHttpAsyncClientEngine setKeystoreType(final KeystoreType keystoreType) {
    this.keystoreType = keystoreType;
    return this;
  }

  public ApacheHttpAsyncClientEngine setKeystorePath(final String keystorePath) {
    this.keystorePath = keystorePath;
    return this;
  }

  private KeyStore readStore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
    try (InputStream keyStoreStream = this.getClass().getResourceAsStream(keystorePath)) {
      KeyStore keyStore = KeyStore.getInstance(keystoreType.name());
      keyStore.load(keyStoreStream, keystorePass.toCharArray());
      return keyStore;
    }
  }

  private void setupCerts() {
    // Trust standard CAs and all self-signed certs

    try {
      this.sslContext = SSLContexts.custom().loadKeyMaterial(readStore(), keyPass.toCharArray()).loadTrustMaterial(new TrustSelfSignedStrategy()).build();
    } catch (KeyStoreException keystoreEx) {
      throw new RuntimeException("Failure Keystore", keystoreEx);
    } catch (KeyManagementException keymanEx) {
      throw new RuntimeException("Failure Keyman", keymanEx);
    } catch (NoSuchAlgorithmException algorithmEx) {
      throw new RuntimeException("Failure", algorithmEx);
    } catch (UnrecoverableKeyException keyEx) {
      throw new RuntimeException("Unrecoverable Key", keyEx);
    } catch (CertificateException ex) {
      throw new RuntimeException("Certificate Exception", ex);
    } catch (IOException ex) {
      throw new RuntimeException("General IO Exception", ex);
    }
    // Allow TLSv1 protocol only
    this.sslSessionStrategy = new SSLIOSessionStrategy(
        sslContext,
        new String[] { SSLSessionProtocols.TLSv1.name() },
        null,
        new NoopHostnameVerifier());

    this.httpAsyncClient = HttpAsyncClients.custom()
        .setSSLStrategy(sslSessionStrategy)
        .build();
  }
  /**
   * Executes a request.
   * @param scheme the scheme of the request
   * @param stereoHttpRequest the request to send
   * @return a {@link StereoHttpRequestHandler}
   */
  public StereoHttpRequestHandler executeRequest(Http.Scheme scheme, StereoHttpRequest<?,?> stereoHttpRequest) {
    final String requestURI = scheme.getProtocol() + stereoHttpRequest.getFullUrl()
        + stereoHttpRequest.getRequestUri();

    LoggingUtil.infoIf(LOG, () -> "AsyncEngine execute async request: " + requestURI);
    HttpUriRequest uriRequest;
    switch(stereoHttpRequest.getRequestMethod()) {
      case GET:
        uriRequest = new HttpGet(requestURI);
        break;
      // create / post are the same
      case CREATE:
      case POST:
        HttpPost postRequest = new HttpPost(requestURI);
        uriRequest = postRequest;
        StereoHttpUtils.copyFormData(stereoHttpRequest, postRequest);
        break;
      case PUT:
        HttpPut put = new HttpPut(requestURI);
        uriRequest = put;
        StereoHttpUtils.copyFormData(stereoHttpRequest, put);
        break;
      case PATCH:
        HttpPatch patch = new HttpPatch(requestURI);
        uriRequest = patch;
        StereoHttpUtils.copyFormData(stereoHttpRequest, patch);
        break;
      case DELETE:
        uriRequest = new HttpDelete(requestURI);
        break;
      case HEAD:
        uriRequest = new HttpHead(requestURI);
        break;
      case OPTIONS:
        uriRequest = new HttpOptions(requestURI);
        break;
      // we'll treat trace / connect the same since apache doesn't offer httpconnect
      case TRACE:
      case CONNECT:
        uriRequest = new HttpTrace(requestURI);
        break;
      default:
        throw new IllegalArgumentException(
            "Could not create the uri request with unsupported request method: "
                + stereoHttpRequest.getRequestMethod()
                + "->" + stereoHttpRequest.getRequestUri());
    }

    StereoHttpUtils.copyHeadersAndCookies(stereoHttpRequest, uriRequest);
    final StereoHttpRequestHandler stereoHttpRequestHandler = new StereoHttpRequestHandler(uriRequest);
    httpAsyncClient.execute(uriRequest, stereoHttpRequestHandler.getApacheHttpCallback());
    return stereoHttpRequestHandler;
  }

  public void start() {
    setupCerts();
    LoggingUtil.infoIf(LOG, () -> "Async Client Engine start");
    this.httpAsyncClient.start();
  }

  public void stop() {
    LoggingUtil.infoIf(LOG, () -> "Async Client Engine stop");
    try {
      httpAsyncClient.close();
    } catch (IOException ex) {
      LOG.error("Exception closing async client", ex);
    }
  }
}

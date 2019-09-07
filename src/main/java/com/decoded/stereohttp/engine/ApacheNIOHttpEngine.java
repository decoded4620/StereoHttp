package com.decoded.stereohttp.engine;

import com.decoded.stereohttp.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.RequestDefaultHeaders;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.protocol.*;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;


/**
 * This is an HTTP Engine for Apache Http NIO
 */
public class ApacheNIOHttpEngine {
  private HttpAsyncRequester requester;
  private int maxOutboundConnectionsPerRoute = 10;
  private int maxOutboundConnections = 10;

  // Create client-side I/O reactor
  private ConnectingIOReactor ioReactor;

  private IOEventDispatch ioEventDispatch;
  // Create HTTP connection pool

  private Logger LOG = LoggerFactory.getLogger(ApacheNIOHttpEngine.class);

  private final BasicNIOConnPool pool;

  public ApacheNIOHttpEngine() {
    this.ioEventDispatch = new DefaultHttpClientIODispatch<>(new HttpAsyncRequestExecutor(),
        ConnectionConfig.DEFAULT);
    try {
      this.ioReactor = new DefaultConnectingIOReactor();
    } catch (IOReactorException ex) {
      throw new RuntimeException("Cannot connect to the reactor", ex);
    }

    // TODO - Support SSL in Stereo HTTP https://github.com/decoded4620/StereoHttp/issues/1
    this.pool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);
    // Limit total number of connections to just two
    this.pool.setDefaultMaxPerRoute(maxOutboundConnectionsPerRoute);
    this.pool.setMaxTotal(maxOutboundConnections);
  }


  public void initialize() {
    // OVERRIDE and set configurations here
    // TODO - @barcher decide which request elements are important.
    // Create HTTP requester
    this.requester = new HttpAsyncRequester(HttpProcessorBuilder.create()
        .add(new RequestContent())
        .add(new RequestAcceptEncoding())
        .add(new RequestDefaultHeaders())
        .add(new RequestTargetHost())
        .add(new RequestConnControl())
        .add(new RequestUserAgent(UserAgents.LINUX_JAVA))
        .add(new RequestExpectContinue(true))
        .build());
  }

  public boolean start() {
    // Ready to go!
    try {
      ioReactor.execute(ioEventDispatch);
    } catch (ConnectionClosedException ex) {
      LOG.error("IO Reactor execution was disconnected: " + ioReactor.getStatus(), ex);
      return false;
    } catch (final InterruptedIOException ex) {
      LOG.error("IO Reactor execution was interrupted: " + ioReactor.getStatus(), ex);
      return false;
    } catch (final IOException ex) {
      LOG.error("IO Reactor execution encountered an I/O error: " + ioReactor.getStatus(), ex);
      return false;
    }

    LoggingUtil.debugIf(LOG, () -> "Reactor started: " + ioReactor.getStatus());
    return true;
  }

  public void shutdown() {
    try {
      LOG.warn("ApacheNIOHttpEngine is shutting down");
      ioReactor.shutdown();
    } catch (IOException ex) {
      LOG.warn("Exception during shutdown", ex);
    }
  }

  public HttpRequest buildReadRequest(StereoHttpRequest<?,?> stereoHttpRequest) {
    LoggingUtil.infoIf(LOG, () -> "Building nio read request >> " + stereoHttpRequest.getRequestMethod()
        .name() + " => " + stereoHttpRequest.getHost() + ':' + stereoHttpRequest.getPort() + stereoHttpRequest
        .getRequestUri());
    final BasicHttpRequest request = new BasicHttpRequest(stereoHttpRequest.getRequestMethod().methodName(),
        stereoHttpRequest.getRequestUri());

    StereoHttpUtils.copyHeadersAndCookies(stereoHttpRequest, request);
    return request;
  }

  public HttpRequest buildWriteRequest(RequestMethod method, StereoHttpRequest<?,?> stereoHttpRequest) {
    LoggingUtil.infoIf(LOG, () -> "Building nio write request >> " + stereoHttpRequest.getRequestMethod()
        .name() + " => " + stereoHttpRequest.getHost() + ':' + stereoHttpRequest.getPort() + stereoHttpRequest
        .getRequestUri());

    BasicHttpEntityEnclosingRequest httpRequest = new BasicHttpEntityEnclosingRequest(method.methodName(),  stereoHttpRequest.getRequestUri());

    StereoHttpUtils.copyHeadersAndCookies(stereoHttpRequest, httpRequest);
    StereoHttpUtils.copyFormData(stereoHttpRequest, httpRequest);

    return httpRequest;
  }

  /**
   * Set the NIO client outbound connection maximum
   *
   * @param maxOutboundConnections the max connections for simultaneous outbound connections
   */
  public void setMaxOutboundConnections(int maxOutboundConnections) {
    this.maxOutboundConnections = maxOutboundConnections;
  }

  /**
   * The max output connections per route
   *
   * @param maxOutboundConnectionsPerRoute the max connections per route.
   */
  public void setMaxOutboundConnectionsPerRoute(int maxOutboundConnectionsPerRoute) {
    this.maxOutboundConnectionsPerRoute = maxOutboundConnectionsPerRoute;
  }


  /**
   * Package private, only called by the HttpClient internally.
   *
   * @return this {@link StereoHttpRequestHandler}
   * @throws IllegalStateException if the http client is not initialized
   */
  public Future<HttpResponse> executeNIORequest(HttpHost httpHost, StereoHttpRequestHandler request) {
    if (requester == null) {
      throw new IllegalStateException("StereoHttpRequest was null, which means the http client was not properly initialized.");
    }
    AbstractAsyncResponseConsumer<HttpResponse> responseConsumer = new BasicAsyncResponseConsumer();
    LOG.info("executing NIO async http request @[" + httpHost.getSchemeName() + httpHost.getHostName() + ":" + httpHost.getPort() + "]");
    return requester.execute(new BasicAsyncRequestProducer(httpHost, request.getRequest()), responseConsumer, pool, HttpCoreContext.create(),
        request.getApacheHttpCallback());
  }
}

package com.decoded.stereohttp;

import com.google.inject.Inject;
import org.apache.http.HttpHost;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.RequestDefaultHeaders;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;


/**
 * Minimal asynchronous HTTP/1.1 client.
 * <p>
 * Please note that this example represents a minimal HTTP client implementation.
 * // TODO - @barcher
 * It does not support HTTPS as is.
 * Need to provide BasicNIOConnPool with a connection factory
 * that supports SSL.
 *
 * <h1>Usage</h1>
 * <pre>
 * StereoHttpClient nioHttpClient;
 *
 * this.nioHttpClient.httpQuery(
 *   "ec2-18-188-69-78.us-east-2.compute.amazonaws.com", 9000,
 *   "GET", "/api/milli/identity/users/get?urn=urn:milli:user:123")
 *     .map(nioResponse -&gt; nioResponse.getMaybeContent()
 *     .ifPresent(content -&gt; LOG.info("Got the content: " + content)))
 *     .exceptionally(ex -&gt; LOG.error("Caught the exception"))
 *     .cancelling(() -&gt; LOG.error("Caught the cancellation"))
 *     .andThen(() -&gt; LOG.info("Doing this after its all complete!"));
 * </pre>
 *
 * @see BasicNIOConnPool#BasicNIOConnPool(ConnectingIOReactor,
 * org.apache.http.nio.pool.NIOConnFactory, int)
 * @see org.apache.http.impl.nio.pool.BasicNIOConnFactory
 */
public class StereoHttpClient {

  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpClient.class);


  // Create client-side I/O reactor
  private ConnectingIOReactor ioReactor;
  // Create HTTP protocol processing chain
  private HttpProcessor httpProcessor;
  // Create client-side HTTP protocol handler
  private HttpAsyncRequestExecutor protocolHandler;

  private IOEventDispatch ioEventDispatch;
  // Create HTTP connection pool
  private BasicNIOConnPool pool;
  private HttpAsyncRequester requester;
  private ExecutorService executorService;
  private int maxOutboundConnectionsPerRoute = 10;
  private int maxOutboundConnections = 10;
  private ClientState state = ClientState.OFFLINE;
  private List<StereoHttpRequest> pendingRequests = new ArrayList<>();

  @Inject
  public StereoHttpClient(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public void setMaxOutboundConnections(int maxOutboundConnections) {
    this.maxOutboundConnections = maxOutboundConnections;
  }

  public void setMaxOutboundConnectionsPerRoute(int maxOutboundConnectionsPerRoute) {
    this.maxOutboundConnectionsPerRoute = maxOutboundConnectionsPerRoute;
  }

  protected void initialize() {
    // OVERRIDE and set configurations here
    LOG.info("Construct StereoHttpClient");
    // TODO - @barcher decide which request elements are important.
    this.httpProcessor = HttpProcessorBuilder.create()
        .add(new RequestContent())
        //        .add(new RequestAuthCache())
        //        .add(new RequestDate())
        //        .add(new RequestAddCookies())
        .add(new RequestAcceptEncoding())
        .add(new RequestDefaultHeaders())
        .add(new RequestTargetHost())
        .add(new RequestConnControl())
        .add(new RequestUserAgent(UserAgents.LINUX_JAVA))
        .add(new RequestExpectContinue(true))
        .build();

    this.protocolHandler = new HttpAsyncRequestExecutor();
    this.ioEventDispatch = new DefaultHttpClientIODispatch<>(protocolHandler, ConnectionConfig.DEFAULT);

    try {
      this.ioReactor = new DefaultConnectingIOReactor();
    } catch (IOReactorException ex) {
      throw new RuntimeException("Cannot connect to the reactor");
    }

    this.pool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);
    // Limit total number of connections to just two
    this.pool.setDefaultMaxPerRoute(maxOutboundConnectionsPerRoute);
    this.pool.setMaxTotal(maxOutboundConnections);

    // Create HTTP requester
    requester = new HttpAsyncRequester(httpProcessor);
    LOG.info("Construct StereoHttpClient Complete, status " + ioReactor.getStatus() + ", routes: " + pool.getRoutes()
        .size());
  }

  /**
   * Returns the current state of the client.
   *
   * @return a {@link ClientState}
   */
  public ClientState getState() {
    return state;
  }

  /**
   * Set the state of the client. Restricted to a Deterministic Finite State Machine
   *
   * @param state the {@link ClientState}
   */
  protected void setState(ClientState state) {
    if (this.state != state) {
      LOG.info("setState( " + state + ")");
      switch (state) {
        case OFFLINE:
          if (getState() == ClientState.TERMINATED) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.TERMINATED + " but was: " + getState());
          }
          break;
        case ERROR:
          this.state = state;
          break;
        case ONLINE:
          if (getState() == ClientState.STARTING) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " but was: " + getState());
          }
          break;
        case STARTING:
          if (getState() == ClientState.OFFLINE) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.OFFLINE + " but was: " + getState());
          }
          break;
        case SHUTTING_DOWN:
          if (getState() == ClientState.STARTING || getState() == ClientState.ONLINE) {
            this.state = state;
          }
        case TERMINATED:
          if (getState() == ClientState.ONLINE) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.ONLINE + " but was: " + getState());
          }
          break;
        default:
          break;
      }
    }
  }

  /**
   * Terminate the client.
   */
  public void terminate() {
    LOG.warn("terminate");
    setState(ClientState.SHUTTING_DOWN);
    try {
      LOG.info("Shutting down I/O reactor");
      ioReactor.shutdown();
    } catch (IOException ex) {
      LOG.warn("IO Reactor may have already shut down");
    }

    LOG.info("StereoHttpClient is dead");
    setState(ClientState.TERMINATED);
  }

  /**
   * Start the non-blocking client on our executor thread.
   */
  public void start() {
    LOG.info("start: " + this.state);
    setState(ClientState.STARTING);

    initialize();

    // Run the I/O reactor in a separate thread
    executorService.submit(() -> {
      setState(ClientState.ONLINE);
      LOG.info("started: " + this.state);
      try {
        // Ready to go!
        ioReactor.execute(ioEventDispatch);
      } catch (final InterruptedIOException ex) {
        LOG.error("IO Reactor execution was interrupted", ex);
        setState(ClientState.TERMINATED);
      } catch (final IOException ex) {
        LOG.error("IO Reactor execution encountered an I/O error: ", ex);
        setState(ClientState.ERROR);
      }
      LOG.info("Nio Http Client shutdown complete.");
    });
  }

  /**
   * Perform a query with host and request
   *
   * @param httpHost the host.
   * @param request  the request.
   */
  private StereoHttpRequest nioRequest(HttpHost httpHost, BasicHttpRequest request) {
    LOG.info("NIO Request to: " + httpHost.toHostString() + "[" + request.toString() + "]");
    return new StereoHttpRequest(pool, requester, httpHost, request);
  }

  private StereoHttpRequest query(String scheme, String host, int port, String method, String uri) {
    final HttpHost httpHost = new HttpHost(host, port, scheme);
    final BasicHttpRequest request = new BasicHttpRequest(method, uri);
    if (this.state == ClientState.ONLINE) {
      return nioRequest(httpHost, request).execute();
    } else if (getState() == ClientState.OFFLINE || getState() == ClientState.STARTING) {
      StereoHttpRequest stereoHttpRequest = nioRequest(httpHost, request);
      pendingRequests.add(stereoHttpRequest);
      return stereoHttpRequest;
    } else {
      throw new IllegalStateException("Cannot invoke query on client when state is: " + this.state);
    }
  }

  /**
   * Http Immutable query object used to handled non-blocking io
   *
   * @param host   the host, e.g. www.milli.com
   * @param port   the port, e.g. 8080
   * @param method the method, e.g. GET
   * @param uri    the uri, e.g. /login
   * @return the NIO Request
   */
  public StereoHttpRequest httpQuery(String host, int port, String method, String uri) {
    return query("http", host, port, method, uri);
  }

  /**
   * Https Immutable query object used to handled non-blocking io
   *
   * @param host   the host, e.g. www.milli.com
   * @param port   the port, e.g. 8080
   * @param method the method, e.g. GET
   * @param uri    the uri, e.g. /login
   * @return the NIO Request
   */
  public StereoHttpRequest httpsQuery(String host, int port, String method, String uri) {
    return query("https", host, port, method, uri);
  }

  public enum ClientState {
    OFFLINE, STARTING, ONLINE, SHUTTING_DOWN, TERMINATED, ERROR
  }

  public static class Cfg {
    public int maxOutboundConnections = 100;
    public int maxOutboundConnectionsPerRoute = 30;
  }
}

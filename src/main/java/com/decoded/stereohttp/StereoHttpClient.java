package com.decoded.stereohttp;

import com.google.inject.Inject;
import org.apache.http.ConnectionClosedException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * Minimal asynchronous HTTP/1.1 client.
 * <p>
 * Please note that this example represents a minimal HTTP client implementation. // TODO - @barcher It does not support
 * HTTPS as is. Need to provide BasicNIOConnPool with a connection factory that supports SSL.
 *
 * <h1>Usage</h1>
 * <pre>
 * StereoHttpClient stereoHttpClient;
 *
 * this.stereoHttpClient.httpQuery("ec2-18-188-69-78.us-east-2.compute.amazonaws.com", 9000, RequestMethod.GET,
 * "/api/identity/users/get?urn=urn:yourdomain:user:1 23", request -&lt; {
 *         request.map(stereoResponse -&lt; stereoResponse.getMaybeContent()
 *             .ifPresent(content -&lt; LOG.info("Stereo Response Content: " + content)))
 *         .exceptionally(ex -&lt; LOG.error("Caught the exception"))
 *         .cancelling(() -&lt; LOG.error("Caught the cancellation"))
 *         .andThen(() -&lt; LOG.info("Doing this after its all complete!"));
 *     });
 * </pre>
 *
 * @see BasicNIOConnPool#BasicNIOConnPool(ConnectingIOReactor, org.apache.http.nio.pool.NIOConnFactory, int)
 * @see org.apache.http.impl.nio.pool.BasicNIOConnFactory
 */
public class StereoHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpClient.class);


  // Create client-side I/O reactor
  private ConnectingIOReactor ioReactor;

  private IOEventDispatch ioEventDispatch;
  // Create HTTP connection pool
  private BasicNIOConnPool pool;
  private HttpAsyncRequester requester;
  private ExecutorService executorService;
  private int maxOutboundConnectionsPerRoute = 10;
  private int maxOutboundConnections = 10;
  private ClientState state = ClientState.OFFLINE;
  private Map<Http.Scheme, List<PendingRequest>> pendingRequestsByScheme = new HashMap<>();

  private boolean initialized = false;

  @Inject
  public StereoHttpClient(ExecutorService executorService) {
    this.executorService = executorService;
  }

  private static void debugIf(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message.get());
    }
  }

  /**
   * Set the NIO client outbound connection maximum
   * @param maxOutboundConnections the max connections for simultaneous outbound connections
   */
  public void setMaxOutboundConnections(int maxOutboundConnections) {
    this.maxOutboundConnections = maxOutboundConnections;
  }

  /**
   * The max output connections per route
   * @param maxOutboundConnectionsPerRoute the max connections per route.
   */
  public void setMaxOutboundConnectionsPerRoute(int maxOutboundConnectionsPerRoute) {
    this.maxOutboundConnectionsPerRoute = maxOutboundConnectionsPerRoute;
  }

  /**
   * Returns the executor service that delves out the threads for http requests.
   *
   * @return the executor service.
   */
  public ExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Provides a hook for subclasses to insert logic before (or after) the Requester, reactor, and connection factory are
   * constructed.
   */
  protected void initialize() {
    if (!initialized) {
      debugIf(() -> "Initializing StereoHttpClient");
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
      initialized = true;
    } else {
      throw new IllegalStateException("Already initialized the Stereo Http Client");
    }
  }

  /**
   * Returns the current state of the client.
   *
   * @return a {@link ClientState}
   *
   * @see ClientState
   */
  protected ClientState getState() {
    return state;
  }

  /**
   * Set the state of the client. Restricted to a Deterministic Finite State Machine.
   * <p>
   * Rules: The State Machine can pass from nodes on the left to nodes on the right. The state begins as "Offline" and can
   * move through the state machine in the following way:
   * <pre>
   * OFFLINE -&gt; STARTING
   * STARTING -&gt; ONLINE
   * STARING || ONLINE -&gt; SHUTTING_DOWN
   * ONLINE || STARTING -&gt; ERROR
   * SHUTTING_DOWN -&gt; TERMINATED
   * ERROR || TERMINATED -&gt; OFFLINE
   * </pre>
   *
   * @param state the {@link ClientState}
   */
  protected void setState(ClientState state) {
    debugIf(() -> "Attempting to Set StereoHttpClient State" + state);
    if (this.state != state) {
      switch (state) {
        case OFFLINE:
          if (getState() == ClientState.TERMINATED || getState() == ClientState.ERROR) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.TERMINATED + " or " + ClientState.ERROR + " but was: " + getState());
          }
          break;
        case ERROR:
          if (getState() == ClientState.STARTING || getState() == ClientState.ONLINE) {
            LOG.error("State was set to ERROR", new RuntimeException("Error"));
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " or " + ClientState.ONLINE + " but " +
                    "was: " + getState());
          }
          break;
        case ONLINE:
          if (getState() == ClientState.STARTING) {
            this.state = state;
            // create the outgoing requests for all schemes.
            // call the request consumer for each caller to query to satisfy the created request.
            pendingRequestsByScheme.forEach((scheme, requests) -> requests.forEach(
                request -> request.requestConsumer.accept(nioRequest(request.host, request.request))));
            pendingRequestsByScheme.clear();
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " but was: " + getState());
          }
          break;
        case STARTING:
          if (!initialized) {
            throw new IllegalStateException(
                "Cannot start the StereoHttpClient before calling the protected initialize() method");
          }

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
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " or " + ClientState.ONLINE + " but was: " + getState());
          }

        case TERMINATED:
          if (getState() == ClientState.SHUTTING_DOWN) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.SHUTTING_DOWN + " but was: " + getState());
          }
          break;
        default:
          break;
      }

      debugIf(() -> "State was changed to " + state);
    }
  }

  /**
   * Terminate the client.
   */
  public void terminate() {
    debugIf(() -> "Terminate Stereo Http Client");
    setState(ClientState.SHUTTING_DOWN);
    try {
      LOG.warn("Shutting down I/O reactor");
      ioReactor.shutdown();
    } catch (IOException ex) {
      LOG.warn("IO Reactor may have already shut down");
    }

    initialized = false;
    setState(ClientState.TERMINATED);
  }

  /**
   * Returns true if the http client can be started.
   * @return a boolean
   */
  public boolean canStart() {
    return state == ClientState.OFFLINE || state == ClientState.TERMINATED;
  }

  /**
   * Start the non-blocking client on our executor thread.
   */
  public void start() {
    debugIf(() -> "Starting StereoHttpClient");
    initialize();

    setState(ClientState.STARTING);

    // Run the I/O reactor in a separate thread
    executorService.submit(() -> {
      setState(ClientState.ONLINE);
      debugIf(() -> "Stereo Client is online");
      try {
        // Ready to go!
        ioReactor.execute(ioEventDispatch);
      } catch (ConnectionClosedException ex) {
        LOG.error("IO Reactor execution was disconnected", ex);
      } catch (final InterruptedIOException ex) {
        LOG.error("IO Reactor execution was interrupted", ex);
        setState(ClientState.TERMINATED);
      } catch (final IOException ex) {
        LOG.error("IO Reactor execution encountered an I/O error: ", ex);
        setState(ClientState.ERROR);
      }
    });
  }

  /**
   * Perform a query with host and request
   *
   * @param httpHost the host.
   * @param request  the request.
   */
  /* package private */ StereoHttpRequest nioRequest(HttpHost httpHost, BasicHttpRequest request) {
    return new StereoHttpRequest(pool, requester, httpHost, request).execute();
  }

  /**
   * Build a Stereo Http RestRequest
   *
   * @param scheme          the scheme
   * @param host            the host
   * @param port            the port
   * @param method          the request method
   * @param uri             the uri
   * @param requestConsumer a consumer for the constructed request.
   */
  private void query(Http.Scheme scheme,
                     String host,
                     int port,
                     RequestMethod method,
                     String uri,
                     Consumer<StereoHttpRequest> requestConsumer
  ) {
    debugIf(() -> "Query: " + method.name() + " - " + scheme + "://" + host + ':' + port + uri);
    final HttpHost httpHost = new HttpHost(host, port, scheme.getProtocol());
    final BasicHttpRequest request = new BasicHttpRequest(method.methodName(), uri);
    if (this.state == ClientState.ONLINE) {
      requestConsumer.accept(nioRequest(httpHost, request));
    } else if (getState() == ClientState.OFFLINE || getState() == ClientState.STARTING) {
      pendingRequestsByScheme.computeIfAbsent(scheme, s -> new ArrayList<>())
          .add(new PendingRequest(httpHost, request, requestConsumer));
    } else {
      throw new IllegalStateException("Cannot invoke query on client when state is: " + this.state);
    }
  }

  /**
   * Http Immutable query object used to handled non-blocking io
   *
   * @param host            the host, e.g. www.milli.com
   * @param port            the port, e.g. 8080
   * @param requestMethod   the method, e.g. GET
   * @param uri             the uri, e.g. /login
   * @param requestConsumer consumer for the created request.
   */
  public void httpQuery(String host,
                        int port,
                        RequestMethod requestMethod,
                        String uri,
                        Consumer<StereoHttpRequest> requestConsumer
  ) {
    query(Http.Scheme.HTTP, host, port, requestMethod, uri, requestConsumer);
  }

  /**
   * Https Immutable query object used to handled non-blocking io
   *
   * @param host            the host, e.g. www.milli.com
   * @param port            the port, e.g. 8080
   * @param method          the method, e.g. GET
   * @param uri             the uri, e.g. /login
   * @param requestConsumer a consumer of the created request.
   */
  public void httpsQuery(String host,
                         int port,
                         RequestMethod method,
                         String uri,
                         Consumer<StereoHttpRequest> requestConsumer
  ) {
    query(Http.Scheme.HTTPS, host, port, method, uri, requestConsumer);
  }

  /**
   * SSH Immutable query
   *
   * @param host            the ssh host
   * @param method          the request method
   * @param uri             the ssh uri
   * @param requestConsumer the consumer for the created request.
   */
  public void sshQuery(String host, RequestMethod method, String uri, Consumer<StereoHttpRequest> requestConsumer) {
    query(Http.Scheme.SSH, host, Http.Scheme.SSH.getDefaultPort(), method, uri, requestConsumer);
  }

  /**
   * FTP Immutable query
   *
   * @param host            the ssh host
   * @param method          the request method
   * @param uri             the ssh uri
   * @param requestConsumer the consumer for the created request.
   */
  public void ftpQuery(String host, RequestMethod method, String uri, Consumer<StereoHttpRequest> requestConsumer) {
    query(Http.Scheme.FTP, host, Http.Scheme.FTP.getDefaultPort(), method, uri, requestConsumer);
  }

  /**
   * SFTP Immutable query
   *
   * @param host            the ssh host
   * @param method          the request method
   * @param uri             the ssh uri
   * @param requestConsumer the consumer for the created request.
   */
  public void sftpQuery(String host, RequestMethod method, String uri, Consumer<StereoHttpRequest> requestConsumer) {
    query(Http.Scheme.SFTP, host, Http.Scheme.SFTP.getDefaultPort(), method, uri, requestConsumer);
  }

  /**
   * States that the client can be in. Following the state machine rules stated in the class docs.
   * @see StereoHttpClient#setState(ClientState)
   */
  public enum ClientState {
    OFFLINE, STARTING, ONLINE, SHUTTING_DOWN, TERMINATED, ERROR
  }

  /**
   * If requests are made prior to being online, they are stored in pending request queue.
   */
  private static final class PendingRequest {
    public HttpHost host;
    public BasicHttpRequest request;
    Consumer<StereoHttpRequest> requestConsumer;

    /**
     * Constructor
     *
     * @param host            host
     * @param request         request
     * @param requestConsumer consumer to call when request is constructed
     */
    public PendingRequest(HttpHost host, BasicHttpRequest request, Consumer<StereoHttpRequest> requestConsumer) {
      debugIf(() -> "Pending Request to " + host.toHostString() + ", " + request.toString());
      this.host = host;
      this.request = request;
      this.requestConsumer = requestConsumer;
    }
  }
}

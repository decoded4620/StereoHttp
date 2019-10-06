package com.decoded.stereohttp;


import com.decoded.stereohttp.engine.ApacheHttpAsyncClientEngine;
import com.decoded.stereohttp.engine.ApacheNIOHttpEngine;
import com.google.inject.Inject;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
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
 * this.stereoHttpClient.stereoApacheNIOReadRequest("ec2-18-188-69-78.us-east-2.compute.amazonaws.com", 9000,
 * RequestMethod.GET,
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


  private ExecutorService executorService;
  private ClientState state = ClientState.OFFLINE;
  private Map<Http.Scheme, List<PendingRequest>> pendingRequestsByScheme = new HashMap<>();

  private ApacheNIOHttpEngine nioHttpEngine;
  private ApacheHttpAsyncClientEngine asyncClientEngine;

  private boolean initialized = false;

  @Inject
  public StereoHttpClient(ApacheNIOHttpEngine nioHttpEngine,
      ApacheHttpAsyncClientEngine asyncClientEngine,
      ExecutorService executorService) {
    this.nioHttpEngine = nioHttpEngine;
    this.asyncClientEngine = asyncClientEngine;
    this.executorService = executorService;
  }

  private static void debugIf(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message.get());
    }
  }

  private static void infoIf(Supplier<String> message) {
    if (LOG.isInfoEnabled()) {
      LOG.info(message.get());
    }
  }

  /**
   * Returns a new stereo http task of the specified type
   *
   * @param tClass  the class
   * @param timeout a timeout value
   * @param <T>     the type
   *
   * @return a Stereo http task.
   */
  public <T> StereoHttpTask<T> task(Class<T> tClass, int timeout) {
    return new StereoHttpTask<>(this, timeout);
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
  protected synchronized void initialize() {
    if (!initialized) {
      infoIf(() -> "Initializing StereoHttpClient");
      nioHttpEngine.initialize();
      asyncClientEngine.start();
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
   * Rules: The State Machine can pass from nodes on the left to nodes on the right. The state begins as "Offline" and
   * can move through the state machine in the following way:
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
          if (this.state == ClientState.TERMINATED || this.state == ClientState.ERROR) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.TERMINATED + " or " + ClientState.ERROR + " but "
                    + "was: " + this.state);
          }
          break;
        case ERROR:
          if (this.state == ClientState.STARTING || this.state == ClientState.ONLINE) {
            LOG.error("State was set to ERROR", new RuntimeException("Error"));
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " or " + ClientState.ONLINE + " but " + "was: " + this.state);
          }
          break;
        case ONLINE:
          if (this.state == ClientState.STARTING) {
            this.state = state;
            // create the outgoing requests for all schemes.
            // call the request consumer for each caller to apacheNIORead to satisfy the created request.
            pendingRequestsByScheme.forEach((scheme, requests) -> requests.forEach(
                request -> nioHttpEngine.executeNIORequest(request.host, request.nioHttpRequest)));
            pendingRequestsByScheme.clear();
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " but was: " + this.state);
          }
          break;
        case STARTING:
          if (!initialized) {
            initialize();
          }

          if (this.state == ClientState.OFFLINE || this.state == ClientState.TERMINATED) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.OFFLINE + " or " + ClientState.TERMINATED + ", " + "but was: " + this.state);
          }

          break;
        case SHUTTING_DOWN:
          if (this.state == ClientState.STARTING || this.state == ClientState.ONLINE) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.STARTING + " or " + ClientState.ONLINE + " but " + "was: " + this.state);
          }
          break;
        case TERMINATED:
          if (this.state == ClientState.SHUTTING_DOWN) {
            this.state = state;
          } else {
            throw new IllegalArgumentException(
                "Client current state expected to be " + ClientState.SHUTTING_DOWN + " but was: " + this.state);
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
  public synchronized void terminate() {
    infoIf(() -> "Terminate Stereo Http Client");
    setState(ClientState.SHUTTING_DOWN);

    nioHttpEngine.shutdown();
    asyncClientEngine.stop();
    initialized = false;
    setState(ClientState.TERMINATED);
  }

  /**
   * Returns true if the http client can be started.
   *
   * @return a boolean
   */
  public boolean canStart() {
    return state == ClientState.OFFLINE;
  }

  /**
   * Start the non-blocking client on our executor thread.
   */
  public synchronized void start() {
    infoIf(() -> "Starting StereoHttpClient");
    setState(ClientState.STARTING);

    // Run the I/O reactor in a separate thread
    executorService.submit(() -> {
      setState(ClientState.ONLINE);
      infoIf(() -> "Stereo Client is online");

      if (!nioHttpEngine.start()) {
        setState(StereoHttpClient.ClientState.ERROR);
        terminate();
      }
    });
  }

  /**
   * Http Immutable apacheNIORead object used to handled non-blocking io
   *
   * @param stereoHttpRequest the request
   * @return StereoHttpRequestHandler
   */
  public StereoHttpRequestHandler stereoApacheNIOReadRequest(StereoHttpRequest stereoHttpRequest) {
    if (stereoHttpRequest.isSecure()) {
      return apacheNIORead(Http.Scheme.HTTPS, stereoHttpRequest);
    } else {
      return apacheNIORead(Http.Scheme.HTTP, stereoHttpRequest);
    }
  }

  /**
   * Write to a host
   *
   * @param stereoHttpRequest a rest request.
   *
   * @return StereoHttpRequestHandler
   * @see StereoHttpClient#stereoApacheNIOReadRequest(StereoHttpRequest)
   */
  public StereoHttpRequestHandler stereoApacheNIOWriteRequest(StereoHttpRequest stereoHttpRequest) {
    if (stereoHttpRequest.isSecure()) {
      return apacheNIOWrite(Http.Scheme.HTTPS, stereoHttpRequest);
    } else {
      return apacheNIOWrite(Http.Scheme.HTTP, stereoHttpRequest);
    }
  }

  public StereoHttpRequestHandler stereoApacheAsyncReadRequest(StereoHttpRequest stereoHttpRequest) {
    if (stereoHttpRequest.isSecure()) {
      return apacheAsyncRead(Http.Scheme.HTTPS, stereoHttpRequest);
    } else {
      return apacheAsyncRead(Http.Scheme.HTTP, stereoHttpRequest);
    }
  }

  public StereoHttpRequestHandler stereoApacheAsyncWriteRequest(StereoHttpRequest stereoHttpRequest) {
    if (stereoHttpRequest.isSecure()) {
      return apacheAsyncWrite(Http.Scheme.HTTPS, stereoHttpRequest);
    } else {
      return apacheAsyncWrite(Http.Scheme.HTTP, stereoHttpRequest);
    }
  }

  /**
   * Creates a new StereoHttpRequestHandler and executes, or stages it.
   *
   * @param scheme  the scheme
   * @param host    the host
   * @param port    the port
   * @param request the request
   */
  private StereoHttpRequestHandler sendHttpRequest(Http.Scheme scheme, String host, int port, HttpRequest request) {
    final HttpHost httpHost = new HttpHost(host, port, scheme.getProtocol());
    infoIf(() -> ">> sending Apache NIO http request: [" + request.getRequestLine()
        .getMethod() + "] " + httpHost.toHostString() + request.getRequestLine().getUri());
    final StereoHttpRequestHandler nioHttpRequest = new StereoHttpRequestHandler(request);
    if (this.state == ClientState.ONLINE) {
      nioHttpEngine.executeNIORequest(httpHost, nioHttpRequest);
      return nioHttpRequest;
    } else if (this.state == ClientState.OFFLINE || this.state == ClientState.STARTING) {
      LOG.warn("Stereo is offline! Staging request " + request.getRequestLine().getUri());
      pendingRequestsByScheme.computeIfAbsent(scheme, s -> new ArrayList<>())
          .add(new PendingRequest(httpHost, nioHttpRequest));

      return nioHttpRequest;
    } else {
      throw new IllegalStateException("Cannot invoke apacheNIORead on client when state is: " + this.state);
    }
  }

  /**
   * Write data in a request, using entities
   *
   * @param scheme            the scheme to apacheNIOWrite
   * @param stereoHttpRequest the rest request.
   */
  private StereoHttpRequestHandler apacheAsyncWrite(Http.Scheme scheme, StereoHttpRequest<?, ?> stereoHttpRequest) {
    if (RequestMethod.isWriteMethod(stereoHttpRequest.getRequestMethod())) {
      return asyncClientEngine.executeRequest(scheme, stereoHttpRequest);
    } else {
      throw new IllegalArgumentException(
          "Cannot perform a write to: " + stereoHttpRequest.getRequestUri() + " with method: " + stereoHttpRequest.getRequestMethod());
    }
  }

  /**
   * Build a Stereo Http StereoHttpRequest
   *
   * @param stereoHttpRequest the request
   * @param scheme            the scheme
   * @return a {@link StereoHttpRequestHandler}
   */
  private StereoHttpRequestHandler apacheAsyncRead(Http.Scheme scheme, StereoHttpRequest stereoHttpRequest) {
    if (!RequestMethod.isWriteMethod(stereoHttpRequest.getRequestMethod())) {
      return asyncClientEngine.executeRequest(scheme, stereoHttpRequest);
    } else {
      throw new IllegalArgumentException(
          "Cannot perform a read from: " + stereoHttpRequest.getRequestUri() + " with method: " + stereoHttpRequest.getRequestMethod());
    }
  }

  /**
   * Write data in a request, using entities
   *
   * @param scheme            the scheme to apacheNIOWrite
   * @param stereoHttpRequest the rest request.
   * @return a {@link StereoHttpRequestHandler}
   */
  private StereoHttpRequestHandler apacheNIOWrite(Http.Scheme scheme, StereoHttpRequest<?, ?> stereoHttpRequest) {
    infoIf(() -> "Capturing outgoing apacheNIOWrite request >> " + stereoHttpRequest.getRequestMethod()
        .name() + " " + scheme + "://" + stereoHttpRequest.getFullUrl() + stereoHttpRequest
        .getRequestUri());
    RequestMethod method = stereoHttpRequest.getRequestMethod();

    if (RequestMethod.isWriteMethod(method)) {
      return sendHttpRequest(scheme, stereoHttpRequest.getHost(), stereoHttpRequest.getPort(),
          nioHttpEngine.buildWriteRequest(method, stereoHttpRequest));
    } else {
      LOG.error("Request Method " + method.name() + " is not a write method!");
      throw new IllegalArgumentException("Exception, method is incorrect");
    }
  }

  /**
   * Build a Stereo Http StereoHttpRequest
   *
   * @param stereoHttpRequest the request
   * @param scheme            the scheme
   */
  private StereoHttpRequestHandler apacheNIORead(Http.Scheme scheme, StereoHttpRequest stereoHttpRequest) {
    if (!RequestMethod.isWriteMethod(stereoHttpRequest.getRequestMethod())) {
      return sendHttpRequest(scheme, stereoHttpRequest.getHost(), stereoHttpRequest.getPort(),
          nioHttpEngine.buildReadRequest(stereoHttpRequest));
    } else {
      LOG.error("Request method " + stereoHttpRequest.getRequestMethod().name() + " is a write request");
      throw new IllegalArgumentException("Request Method is incorrect");
    }
  }

  /**
   * States that the client can be in. Following the state machine rules stated in the class docs.
   *
   * @see StereoHttpClient#setState(ClientState)
   */
  public enum ClientState {
    OFFLINE, STARTING, ONLINE, SHUTTING_DOWN, TERMINATED, ERROR
  }

  /**
   * If requests are made prior to being online, they are stored in pending request queue.
   */
  private static final class PendingRequest {
    StereoHttpRequestHandler nioHttpRequest;
    HttpHost host;

    /**
     * Constructor
     *
     * @param nioHttpRequest the pending request
     */
    public PendingRequest(HttpHost host, StereoHttpRequestHandler nioHttpRequest) {
      this.host = host;
      this.nioHttpRequest = nioHttpRequest;
    }
  }
}

package com.decoded.stereohttp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.decoded.stereohttp.LoggingUtil.debugIf;
import static com.decoded.stereohttp.LoggingUtil.infoIf;


/**
 * Http Task, which returns a type of type T The Stereo Task attempts to convert the raw result (which is expected to be
 * in JSON format) into the type T, along with any custom encoder or decoders required.
 *
 * <h2>Usage</h2>
 * <pre>
 *   new StereoHttpTask&lt;MyRecordType&gt;(MyRecordType.class, myStereoClient, 2000).execute(RestRequestBuilders...
 *   .build());
 * </pre>
 *
 * @param <T> the type to create using the underlying Http Response data  ({@link StereoResponse}) from a {@link
 *            StereoHttpRequestHandler}.
 */
public class StereoHttpTask<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpTask.class);

  private static final AtomicLong NEXT_TASK_ID = new AtomicLong(0L);
  // used to accurately track operation status
  private static final AtomicLong MAX_LATENCY = new AtomicLong(Integer.MIN_VALUE);
  private static final AtomicLong MIN_LATENCY = new AtomicLong(0);
  private static final AtomicLong CONCURRENT_REQUESTS = new AtomicLong(0);
  private static final AtomicLong MAX_CONCURRENT_REQUESTS = new AtomicLong(0);
  private static final AtomicLong TOTAL_REQUESTS = new AtomicLong(0);

  private final StereoHttpClient stereoHttpClient;
  private final int timeout;
  private final long taskId;
  private boolean useAsyncHttpClient = true;
  private Map<Integer, Integer> statusRetries = new ConcurrentHashMap<>();

  /**
   * StereoHttpTask
   *
   * @param stereoHttpClient the stereo client.
   * @param timeout          the timeout.
   */
  public StereoHttpTask(StereoHttpClient stereoHttpClient, int timeout) {
    this.stereoHttpClient = stereoHttpClient;
    this.timeout = timeout;
    this.taskId = NEXT_TASK_ID.getAndIncrement();
  }

  /**
   * Total requests that have been processed by {@link StereoHttpTask}
   *
   * @return a long
   */
  public static long totalRequests() {
    return TOTAL_REQUESTS.get();
  }

  /**
   * Maximum latency incurred by a request
   *
   * @return the value in milliseconds
   */
  public static long maxLatency() {
    return MAX_LATENCY.get();
  }

  /**
   * Minimum latency incurred by a request
   *
   * @return the value in milliseconds
   */
  public static long minLatency() {
    return MIN_LATENCY.get();
  }

  /**
   * The maximum value of concurrency in this client.
   *
   * @return the number of maximum concurrent requests
   */
  public static long maxConcurrentRequests() {
    return MAX_CONCURRENT_REQUESTS.get();
  }

  /**
   * Total number of concurrent requests in progress right now.
   *
   * @return the number of concurrent requests.
   */
  public static long concurrentRequests() {
    return CONCURRENT_REQUESTS.get();
  }

  /**
   * The task id.
   *
   * @return the identifier of this stereo task.
   */
  public long getTaskId() {
    return taskId;
  }

  private String taskLabel() {
    return "[stereotask " + taskId + "] ";
  }

  /**
   * Execute the restRequest.
   *
   * @param type    the class for the type of response that is being returned by this class. (unserialized type).
   * @param request the restRequest
   * @param <ID_T>  the identifier type for the request. Used as a pass through here.
   *
   * @return a {@link CompletableFuture} of a {@link Feedback}&lt;T&gt;
   */
  public <ID_T> CompletableFuture<Feedback<T>> execute(Class<T> type, StereoHttpRequest<T, ID_T> request) {
    return executeRequest(request, type, null);
  }

  /**
   * Execute a batch request.
   *
   * @param typeReference the type reference.
   * @param request       a request.
   * @param <ID_T>        the id type.
   *
   * @return a completable future.
   */
  public <ID_T> CompletableFuture<Feedback<List<T>>> executeBatch(TypeReference<List<T>> typeReference,
      StereoHttpRequest<List<T>, ID_T> request) {
    debugIf(LOG, () -> taskLabel() + "executeBatch(): " + timeout + " ms");
    return executeRequest(request, null, typeReference);
  }

  private void setFeedbackResponseError(Feedback feedback, int status, String content, String message) {
    infoIf(LOG, () -> taskLabel() + "Http Response Feedback Error: " + status + " -> " + message);
    feedback.setError(status, content, new StereoResponseException(status, message));
  }

  private void setFeedbackRequestError(Feedback feedback, int status, String content, String message) {
    infoIf(LOG, () -> taskLabel() + "Http Request Feedback Error: " + status + " -> " + message);
    feedback.setError(status, content, new StereoRequestException(status, message));
  }

  /**
   * Process the error type feedback
   *
   * @param <X>        the error response
   * @param feedback   the feedback
   * @param statusCode the status
   * @param response   the response
   * @param message    the error message
   */
  private <X> void setFeedbackResponseError(Feedback<X> feedback,
      int statusCode,
      StereoResponse response,
      final String message,
      Throwable cause) {
    infoIf(LOG, () -> taskLabel() + "Http Response Exception: " + cause.getMessage());
    feedback.setError(statusCode, response == null ? "" : response.getContent(),
        new StereoResponseException(statusCode, message, cause));
  }

  /**
   * Process the error type feedback
   *
   * @param <X>        the error response
   * @param feedback   the feedback
   * @param statusCode the status
   * @param response   the response
   * @param message    the message
   */
  private <X> void setFeedbackRequestError(Feedback<X> feedback,
      int statusCode,
      StereoResponse response,
      final String message,
      Throwable cause) {
    infoIf(LOG, () -> taskLabel() + "Http Request Exception: " + cause.getMessage());
    feedback.setError(statusCode, response == null ? "" : response.getContent(),
        new StereoRequestException(statusCode, message, cause));
  }

  /**
   * Process the ok type feedback
   *
   * @param statusCode    the code
   * @param clazz         the class to deserialize to
   * @param typeReference the type to deserialze to
   * @param response      the response
   * @param <X>           the type
   */
  private <X> void buildFeedbackFor2xxResponse(int statusCode,
      Class<X> clazz,
      Feedback<X> feedback,
      TypeReference<X> typeReference,
      StereoResponse response) {
    debugIf(LOG, () -> taskLabel() + "Process ok response feedback: " + statusCode);
    if (response == null) {
      setFeedbackResponseError(feedback, HttpStatus.SC_INTERNAL_SERVER_ERROR, "",
          "No Stereo Response was provided for processing!");
    } else {
      if (response.getContent() != null) {
        if (clazz == null && typeReference == null) {
          setFeedbackRequestError(feedback, HttpStatus.SC_BAD_REQUEST, response.getContent(),
              "Could not detect required type for deserialization");
        } else {
          X deserialized = null;
          try {
            if (clazz != null) {
              deserialized = new ObjectMapper().readValue(response.getContent(), clazz);
            } else {
              deserialized = new ObjectMapper().readValue(response.getContent(), typeReference);
            }
            feedback.setSuccess(statusCode, deserialized, response.getContent());
          } catch (IOException ex) {
            LOG.warn(taskLabel() + "An Exception occurred while mapping the response: ", ex);
            feedback.setSuccess(statusCode, null, response.getContent());
          }
        }
      } else {
        debugIf(LOG, () -> "Successfully processed feedback response:" + response.getRawHttpResponse()
            .getStatusLine()
            .toString());
        feedback.setSuccess(statusCode, null, response.getContent());
      }
    }
  }

  /**
   * Tracking bookend start
   *
   * @return a long, the start time of a request in milliseconds.
   */
  private long requestStarted() {
    final long start = System.currentTimeMillis();
    debugIf(LOG, () -> taskLabel() + "Request started at " + start + " ms  >>>> >>>> >>>> >>>> >>>>");
    long concurrentRequests = CONCURRENT_REQUESTS.incrementAndGet();

    if (concurrentRequests > MAX_CONCURRENT_REQUESTS.get()) {
      MAX_CONCURRENT_REQUESTS.set(concurrentRequests);
    }

    return start;
  }

  /**
   * Tracking bookend complete
   *
   * @param startVal the start value of the request in milliseconds.
   */
  private void requestCompleted(long startVal) {
    CONCURRENT_REQUESTS.decrementAndGet();
    long delta = System.currentTimeMillis() - startVal;
    debugIf(LOG, () -> taskLabel() + "Request completed in " + delta + " ms <<<< <<<< <<<< <<<< <<<<");

    if (delta < MIN_LATENCY.get()) {
      debugIf(LOG, () -> "Min latency improved!! " + delta);
      MIN_LATENCY.set(delta);
    }

    if (delta > MAX_LATENCY.get()) {
      debugIf(LOG, () -> "Max latency degradation!!: " + delta);
      MAX_LATENCY.set(delta);
    }

    TOTAL_REQUESTS.incrementAndGet();
  }

  private boolean is2xxStatusCode(int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  private boolean isErrorCode(int statusCode) {
    return !is2xxStatusCode(statusCode) && statusCode >= 400 && statusCode < 600;
  }

  /**
   * Returns the request line for a stereo request for output
   *
   * @param stereoHttpRequest the request
   *
   * @return a string, the request line for debugging / logging output.
   */
  private String getRequestLine(StereoHttpRequest stereoHttpRequest) {
    return stereoHttpRequest.getRequestMethod()
        .name() + " " + stereoHttpRequest.getFullUrl() + stereoHttpRequest.getRequestUri();
  }

  /**
   * Internal method to run the request.
   *
   * @param httpRequest a {@link StereoHttpRequest}
   * @param <ID_T>      the type of identifier used to locate the  content for the query.
   *
   * @return the type specified feedback object for this task.
   */
  private <X, ID_T> CompletableFuture<Feedback<X>> executeRequest(StereoHttpRequest<X, ID_T> httpRequest,
      Class<X> clazz,
      TypeReference<X> typeReference) {
    CompletableFuture<Feedback<X>> requestCompleteFuture = new CompletableFuture<>();
    debugIf(LOG, () -> taskLabel() + "execute request: " + httpRequest.getRequestUri());

    Runnable feedbackRunner = () -> {
      long start = requestStarted();

      debugIf(LOG, () -> taskLabel() + "StereoHttp Request running: [" + httpRequest.getRequestUri() + "]");

      StereoHttpRequestHandler stereoRequestHandler;
      if (useAsyncHttpClient) {
        if (RequestMethod.isWriteMethod(httpRequest.getRequestMethod())) {
          stereoRequestHandler = stereoHttpClient.stereoApacheAsyncWriteRequest(httpRequest);
        } else {
          stereoRequestHandler = stereoHttpClient.stereoApacheAsyncReadRequest(httpRequest);
        }
      } else {
        if (RequestMethod.isWriteMethod(httpRequest.getRequestMethod())) {
          stereoRequestHandler = stereoHttpClient.stereoApacheNIOWriteRequest(httpRequest);
        } else {
          stereoRequestHandler = stereoHttpClient.stereoApacheNIOReadRequest(httpRequest);
        }
      }

      Feedback<X> feedback = new Feedback<>();
      debugIf(LOG, () -> taskLabel() + "Stereo Request Start: [" + httpRequest.getRequestUri() + "]");
      stereoRequestHandler.map(response -> {
        debugIf(LOG,
            () -> taskLabel() + "Stereo Response: [" + httpRequest.getRequestUri() + " (" + response.getRawHttpResponse()
                .getStatusLine()
                .getStatusCode() + ")]: \n---\n" + response.getContent() + "\n---");

        final int statusCode = response.getRawHttpResponse().getStatusLine().getStatusCode();
        debugIf(LOG,
            () -> taskLabel() + "StereoHttp Response captured: " + httpRequest.getRequestUri() + " => " + statusCode);

        if (is2xxStatusCode(statusCode)) {
          buildFeedbackFor2xxResponse(statusCode, clazz, feedback, typeReference, response);
        } else {
          if (isErrorCode(statusCode)) {
            setFeedbackRequestError(feedback, statusCode, response.getContent(),
                "Non 2xx Status: " + statusCode + "->" + getRequestLine(httpRequest));
          } else {
            infoIf(LOG, () -> "Non Error, Non-2xx status: " + statusCode + " for url " + getRequestLine(httpRequest));
            feedback.setSuccess(statusCode, null, response.getContent());
          }
        }

      }).exceptionally(ex -> {
        LOG.warn(taskLabel() + "Stereo Raw Http Request Exception: " + ex.getClass().getName(), ex);
        if (ex.getCause() instanceof ConnectException) {
          setFeedbackRequestError(feedback, HttpStatus.SC_BAD_GATEWAY, null,
              "Bad Gateway Error occurred calling url " + getRequestLine(httpRequest), ex);
        } else {
          setFeedbackRequestError(feedback, HttpStatus.SC_INTERNAL_SERVER_ERROR, null,
              "Internal Server Error occurred calling url " + getRequestLine(httpRequest), ex);
        }
      }).cancelling(() -> {
        infoIf(LOG, () -> taskLabel() + "Stereo Request Cancelled calling url " + getRequestLine(httpRequest));
        feedback.cancel();
      });

      infoIf(LOG, () -> taskLabel() + "Waiting for StereoHttp request to complete..." + httpRequest.getRequestUri());
      if (!feedback.await(timeout, TimeUnit.MILLISECONDS)) {
        infoIf(LOG, () -> taskLabel() + "Request Timed out! " + getRequestLine(httpRequest));
        setFeedbackRequestError(feedback, HttpStatus.SC_GATEWAY_TIMEOUT, "", "Request Timed out!");
      }

      requestCompleted(start);

      // user should have data here, or null
      requestCompleteFuture.complete(feedback);
    };

    try {
      stereoHttpClient.getExecutorService().submit(feedbackRunner);
    } catch (RejectedExecutionException ex) {
      LOG.error(taskLabel() + "Could not schedule stereohttp request, because: ", ex);
      requestCompleteFuture.completeExceptionally(ex);
    }

    return requestCompleteFuture;
  }
}

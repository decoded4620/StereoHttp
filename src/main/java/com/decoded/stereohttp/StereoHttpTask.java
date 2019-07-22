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
import java.util.function.Consumer;
import java.util.function.Supplier;


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
 *            StereoHttpRequest}.
 */
public class StereoHttpTask<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpTask.class);

  // used to accurately track operation status
  private static final AtomicLong MAX_LATENCY = new AtomicLong(Integer.MIN_VALUE);
  private static final AtomicLong MIN_LATENCY = new AtomicLong(0);
  private static final AtomicLong CONCURRENT_REQUESTS = new AtomicLong(0);
  private static final AtomicLong MAX_CONCURRENT_REQUESTS = new AtomicLong(0);
  private static final AtomicLong TOTAL_REQUESTS = new AtomicLong(0);


  private final StereoHttpClient stereoHttpClient;
  private final int timeout;
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
  }

  // helper for efficient debug logging
  private static void debugIf(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message.get());
    }
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
   * Execute the restRequest.
   *
   * @param type the class for the type of response that is being returned by this class. (unserialized type).
   * @param request the restRequest
   * @param <ID_T>  the identifier type for the request. Used as a pass through here.
   *
   * @return a {@link CompletableFuture} of a {@link Feedback}&lt;T&gt;
   */
  public <ID_T> CompletableFuture<Feedback<T>> execute(Class<T> type, RestRequest<T, ID_T> request) {
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
  public <ID_T> CompletableFuture<Feedback<List<T>>> executeBatch(TypeReference typeReference,
      RestRequest<List<T>, ID_T> request) {
    debugIf(() -> "executeBatch(): " + timeout + " ms");
    return executeRequest(request, null, typeReference);
  }

  private void setFeedbackResponseError(Feedback feedback, int status, String content, String message) {
    LOG.info("Http Response Feedback Error: " + status + " -> " + message);
    feedback.setError(status, content, new StereoResponseException(status, message));
  }

  private void setFeedbackRequestError(Feedback feedback, int status, String content, String message) {
    LOG.info("Http Request Feedback Error: " + status + " -> " + message);
    feedback.setError(status, content, new StereoRequestException(status, message));
  }

  /**
   * Process the error type feedback
   *
   * @param <X>        the error response
   * @param feedback   the feedback
   * @param statusCode the status
   * @param response   the response
   * @param message the error message
   */
  private <X> void setFeedbackResponseError(Feedback<X> feedback,
      int statusCode,
      StereoResponse response,
      final String message,
      Throwable cause) {
    LOG.info("Http Response Exception: " + cause.getMessage());
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
    LOG.info("Http Request Exception: " + cause.getMessage());
    feedback.setError(statusCode, response == null ? "" : response.getContent(),
        new StereoRequestException(statusCode, message, cause));
  }

  /**
   * Process the ok type feedback
   *
   * @param statusCode    the code
   * @param feedback      the feedback object
   * @param clazz         the class to deserialize to
   * @param typeReference the type to deserialze to
   * @param response      the response
   * @param <X>           the type
   */
  private <X> void process2xxResponseFeedback(int statusCode,
      Feedback<X> feedback,
      Class<X> clazz,
      TypeReference<X> typeReference,
      StereoResponse response) {
    debugIf(() -> "Process ok response feedback: " + statusCode);
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
            LOG.info("An Exception occurred while mapping the response: ", ex);
            feedback.setSuccess(statusCode, null, response.getContent());
          }
        }
      } else {
        debugIf(() -> "Successfully processed feedback response:" + response.getRawHttpResponse()
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
    debugIf(() -> "Request started at " + start + " ms  >>>> >>>> >>>> >>>> >>>>");
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
    debugIf(() -> "Request completed in " + delta + " ms <<<< <<<< <<<< <<<< <<<<");

    if (delta < MIN_LATENCY.get()) {
      debugIf(() -> "Min latency improved!! " + delta);
      MIN_LATENCY.set(delta);
    }

    if (delta > MAX_LATENCY.get()) {
      debugIf(() -> "Max latency degradation!!: " + delta);
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
   * Internal method to run the request.
   *
   * @param httpRequest a {@link RestRequest}
   * @param <ID_T>      the type of identifier used to locate the  content for the query.
   *
   * @return the type specified feedback object for this task.
   */
  private <X, ID_T> CompletableFuture<Feedback<X>> executeRequest(RestRequest<X, ID_T> httpRequest,
      Class<X> clazz,
      TypeReference<X> typeReference) {
    CompletableFuture<Feedback<X>> requestCompleteFuture = new CompletableFuture<>();
    debugIf(() -> "Perform Stereo request: " + httpRequest.getRequestUri());

    Runnable feedbackRunner = () -> {
      long start = requestStarted();
      Feedback<X> feedback = new Feedback<>();

      debugIf(() -> "Stereo Request Enqueue: [" + httpRequest.getRequestUri() + "]");

      // response callback
      Consumer<StereoResponse> httpResponseCallback = (stereoResponse) -> {
        final int statusCode = stereoResponse.getRawHttpResponse().getStatusLine().getStatusCode();
        debugIf(() -> "Http Response captured: " + httpRequest.getRequestUri() + " => " + statusCode);

        if(is2xxStatusCode(statusCode)) {
          process2xxResponseFeedback(statusCode, feedback, clazz, typeReference, stereoResponse);
        } else {
          if(isErrorCode(statusCode)) {
            setFeedbackRequestError(feedback, statusCode, stereoResponse.getContent(), "Non 2xx Status: " + statusCode + "->" + httpRequest.getRequestUri());
          } else {
            LOG.info("Non Error, Non-2xx status: " + statusCode);
            feedback.setSuccess(statusCode, null, stereoResponse.getContent());
          }
        }
      };

      Consumer<StereoHttpRequest> requestCreateCallback = stereoRequest -> {
        debugIf(() -> "Stereo Request Start: [" + httpRequest.getRequestUri() + "]");
        stereoRequest.map(response -> {
          debugIf(() -> "Stereo Response: [" + httpRequest.getRequestUri() + " (" + response.getRawHttpResponse()
              .getStatusLine()
              .getStatusCode() + ")]: \n---\n" + response.getContent() + "\n---");
          httpResponseCallback.accept(response);
        }).exceptionally(ex -> {
          LOG.warn("Stereo Raw Http Request Exception: " + ex.getClass().getName(), ex);
          if(ex.getCause() instanceof ConnectException) {
            setFeedbackRequestError(feedback, HttpStatus.SC_FORBIDDEN, null, "Exception occurred: ", ex);
          } else {
            setFeedbackRequestError(feedback, HttpStatus.SC_INTERNAL_SERVER_ERROR, null, "Exception occurred: ", ex);
          }
        }).cancelling(() -> {
          debugIf(() -> "Stereo Request Cancelled");
          feedback.cancel();
        });
      };

      if (RequestMethod.isWriteMethod(httpRequest.getRequestMethod())) {
        stereoHttpClient.stereoWriteRequest(httpRequest, requestCreateCallback, httpRequest::getBody);
      } else {
        stereoHttpClient.stereoReadRequest(httpRequest, requestCreateCallback);
      }

      try {
        LOG.info("Waiting for request to complete..." + httpRequest.getRequestUri());
        if (!feedback.await(timeout, TimeUnit.MILLISECONDS)) {
          LOG.info("Request Timed out! " + httpRequest.getRequestUri());
          setFeedbackRequestError(feedback, HttpStatus.SC_INTERNAL_SERVER_ERROR, "", "Request Timed out!");
        }
      } catch (InterruptedException ex) {
        LOG.info("Request Interrupted: " + httpRequest.getRequestUri() + ", error: " + ex.getMessage());
        setFeedbackRequestError(feedback, HttpStatus.SC_INTERNAL_SERVER_ERROR, "", "Http Request was interrupted");
      }

      requestCompleted(start);

      LOG.info("Request Completed!");
      // user should have data here, or null
      requestCompleteFuture.complete(feedback);
    };

    try {
      stereoHttpClient.getExecutorService().submit(feedbackRunner);
    } catch (RejectedExecutionException ex) {
      LOG.error("Could not schedule the request, stereo http client executor service threw an exception: ", ex);
      requestCompleteFuture.complete(null);
    }

    return requestCompleteFuture;
  }
}
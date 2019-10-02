package com.decoded.stereohttp;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * This is a Stereo Http Object that accepts both the result and / or any exceptions during executions, to propagate to
 * the caller.
 *
 * @param <T> the target type returned by the request.
 */
public class Feedback<T> {
  private static final Logger LOG = LoggerFactory.getLogger(Feedback.class);

  private T deserializedContent;
  private int status;
  private String serializedContent;
  private Throwable exception;
  private boolean cancelled;

  private CountDownLatch latch = new CountDownLatch(1);

  public Feedback() {
  }

  public boolean failed() {
    return exception != null && !cancelled;
  }

  public Feedback<T> setSuccess(int status, T deserializedContent, String serializedContent) {
    this.status = status;
    this.deserializedContent = deserializedContent;
    this.serializedContent = serializedContent;

    LoggingUtil.debugIf(LOG, () -> "Stereo Feedback Success(" + status + ", " + (deserializedContent == null
        ? ""
        : deserializedContent.getClass().getName()) + ")");
    latch.countDown();
    return this;
  }

  public Feedback<T> setError(int status, String serializedContent, Throwable cause) {
    LOG.warn("Stereo Feedback Error(" + status + ", " + (serializedContent == null
        ? ""
        : serializedContent.getClass().getName()) + ")", cause);
    this.status = status;
    this.serializedContent = serializedContent;
    this.exception = cause;
    latch.countDown();
    return this;
  }

  public Feedback<T> cancel() {
    LOG.warn("Stereo Feedback Cancel()");
    this.cancelled = true;
    this.serializedContent = "{ \"error\":\"Request cancelled\"}";
    this.status = HttpStatus.SC_REQUEST_TIMEOUT;
    latch.countDown();
    return this;
  }

  public int getStatus() {
    return status;
  }

  /**
   * Wait for feedback to be complete.
   *
   * @param timeout  number of time units to wait
   * @param timeUnit the time unit, e.g. Milliseconds
   *
   * @return true if we were able to wait out the entire request.
   */
  public boolean await(long timeout, TimeUnit timeUnit) {
    try {
      return latch.await(timeout, timeUnit);
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while waiting");
      return false;
    }
  }

  public Optional<T> getDeserializedContent() {
    return Optional.ofNullable(deserializedContent);
  }

  public String getSerializedContent() {
    return serializedContent;
  }

  public Throwable getException() {
    return exception;
  }

  public boolean isCancelled() {
    return cancelled;
  }
}

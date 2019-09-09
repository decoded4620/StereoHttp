package com.decoded.stereohttp;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;


/**
 * This is a Stereo Http Object that accepts both the result and / or any exceptions during executions, to propagate
 * to the caller.
 *
 * @param <T> the target type returned by the request.
 */
public class Feedback<T> extends CountDownLatch {
  private static final Logger LOG = LoggerFactory.getLogger(Feedback.class);

  private T deserializedContent;
  private int status;
  private String serializedContent;
  private Throwable exception;
  private boolean cancelled;

  public Feedback() {
    super(1);
  }

  public boolean failed() {
    return exception != null && !cancelled;
  }

  public Feedback<T> setSuccess(int status,  T deserializedContent, String serializedContent) {
    this.status = status;
    this.deserializedContent = deserializedContent;
    this.serializedContent = serializedContent;

    debugIf(() -> "Stereo Feedback Success(" + status + ", " + (deserializedContent == null
        ? ""
        : deserializedContent.getClass().getName()) + ")");
    countDown();
    return this;
  }

  public Feedback<T> setError(int status, String serializedContent, Throwable cause) {
    LOG.warn("Stereo Feedback Error(" + status + ", " + (serializedContent == null
        ? ""
        : serializedContent.getClass().getName()) + ")", cause);
    this.status = status;
    this.serializedContent = serializedContent;
    this.exception = cause;
    countDown();
    return this;
  }

  public Feedback<T> cancel() {
    LOG.warn("Stereo Feedback Cancel()");
    this.cancelled = true;
    this.serializedContent = "{ \"error\":\"Request cancelled\"}";

    this.status = HttpStatus.SC_REQUEST_TIMEOUT;
    countDown();
    return this;
  }

  private static void debugIf(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{" + Thread.currentThread().getName() + "}:" + message.get());
    }
  }

  @Override
  public void countDown() {
    debugIf(() -> "Count Down from: " + this.getCount());
    super.countDown();
  }

  public int getStatus() {
    return status;
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

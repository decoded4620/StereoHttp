package com.decoded.stereohttp;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


/**
 * the HttpResponseWrapper for Apache
 */
public class StereoResponseImpl implements StereoResponse {

  private static final Logger LOG = LoggerFactory.getLogger(StereoResponseImpl.class);
  private final HttpResponse rawHttpResponse;
  private final HttpEntity rawHttpEntity;
  private final String content;
  private final String contentType;
  private final String encoding;
  private final boolean isStreaming;
  private final boolean isChunked;
  private final boolean isRepeatable;

  /**
   * Package private Constructor. This object is created internally and returned.
   * @param rawHttpResponse the HttpResponse from apache
   */
  StereoResponseImpl(HttpResponse rawHttpResponse) {
    this.rawHttpResponse = rawHttpResponse;
    this.rawHttpEntity = rawHttpResponse.getEntity();
    this.encoding = String.valueOf(this.rawHttpEntity.getContentEncoding());
    this.contentType = String.valueOf(this.rawHttpEntity.getContentType());
    this.isStreaming = this.rawHttpEntity.isStreaming();
    this.isChunked = this.rawHttpEntity.isChunked();
    this.isRepeatable = this.rawHttpEntity.isRepeatable();
    this.content = StereoHttpUtils.getContent(rawHttpResponse).orElse(null);
  }

  @Override
  public Optional<String> getMaybeContent() {
    return Optional.ofNullable(content);
  }

  @Override
  public boolean isRepeatable() {
    return isRepeatable;
  }

  @Override
  public boolean isStreaming() {
    return isStreaming;
  }

  @Override
  public boolean isChunked() {
    return isChunked;
  }

  @Override
  public String getContentType() {
    return contentType;
  }

  @Override
  public String getContent() {
    return content;
  }

  @Override
  public String getEncoding() {
    return encoding;
  }

  @Override
  public int getResponseLength() {
    return content == null ? 0 : content.length();
  }

  @Override
  public long getContentLength() {
    return rawHttpResponse.getEntity().getContentLength();
  }

  @Deprecated
  @Override
  public HttpResponse getRawHttpResponse() {
    return rawHttpResponse;
  }
}

package com.decoded.stereohttp;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;

import java.util.Optional;


/**
 * response interface for interacting with Http Response content.
 */
public interface StereoResponse {
  /**
   * Null protected accessor for content
   * @return Optional string
   */
  Optional<String> getMaybeContent();

  /**
   * <code>true</code> if repeatable content
   * @return boolean
   */
  boolean isRepeatable();

  /**
   * <code>true</code> if streaming content
   * @return boolean
   */
  boolean isStreaming();

  /**
   * <code>true</code> if chunked content
   * @return boolean
   */
  boolean isChunked();

  /**
   * The content type as detected from the response header.
   * @return String
   */
  String getContentType();


  /**
   * The raw content, which may be null.
   * @return String
   */
  String getContent();

  /**
   * Content encoding as detected from the response header.
   * @return String
   */
  String getEncoding();

  /**
   * Length from the response data
   * @return int
   */
  int getResponseLength();

  /**
   * Content length from the http entity
   * @return a long
   */
  long getContentLength();

  /**
   * The raw {@link HttpEntity}
   * @return The raw Http entity.
   */
  @Deprecated
  HttpEntity getRawHttpEntity();

  /**
   * The Raw {@link HttpResponse} from apache
   * @return HttpResponse
   */
  @Deprecated
  HttpResponse getRawHttpResponse();
}

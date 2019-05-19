package com.decoded.stereohttp;

/**
 * Http Request Methods
 */
public enum RequestMethod {
  GET("GET"),
  HEAD("HEAD"),
  TRACE("TRACE"),
  POST("POST"),
  PUT("PUT"),
  DELETE("DELETE"),
  CREATE("CONNECT");

  private final String requestType;
  RequestMethod(String type) {
    this.requestType = type;
  }

  public String methodName() {
    return requestType;
  }
}
package com.decoded.stereohttp;

/**
 * Http RestRequest Methods for http 1.1
 */
public enum RequestMethod {
  GET("GET"),
  HEAD("HEAD"),
  TRACE("TRACE"),
  POST("POST"),
  PUT("PUT"),
  DELETE("DELETE"),
  CREATE("CREATE"),
  CONNECT("CONNECT"),
  OPTIONS("OPTIONS");

  private final String requestType;

  RequestMethod(String type) {
    this.requestType = type;
  }

  public static boolean isWriteMethod(RequestMethod method) {
    return method.equals(POST) || method.equals(PUT) || method.equals(CREATE);
  }

  public String methodName() {
    return requestType;
  }
}

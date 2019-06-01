package com.decoded.stereohttp;

/**
 * Request Exceptions, with codes.
 */
public class StereoRequestException extends RuntimeException {
  private final int code;
  public StereoRequestException(int code, String message) {
    super(message);
    this.code = code;
  }

  public StereoRequestException(int code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}

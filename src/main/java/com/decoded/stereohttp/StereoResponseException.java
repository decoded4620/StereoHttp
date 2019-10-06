package com.decoded.stereohttp;

public class StereoResponseException extends RuntimeException {
  private final int code;
  public StereoResponseException(int code, String message) {
    super(message);
    this.code = code;
  }

  public StereoResponseException(int code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}

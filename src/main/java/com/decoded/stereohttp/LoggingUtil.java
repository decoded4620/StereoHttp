package com.decoded.stereohttp;

import org.slf4j.Logger;

import java.util.function.Supplier;


/**
 * Logging util
 */
public class LoggingUtil {
  // helper for efficient debug logging
  public static void debugIf(Logger log, Supplier<String> message) {
    if (log.isDebugEnabled()) {
      log.debug(message.get());
    }
  }

  // helper for efficient info logging
  public static void infoIf(Logger log, Supplier<String> message) {
    if (log.isInfoEnabled()) {
      log.info(message.get());
    }
  }
}

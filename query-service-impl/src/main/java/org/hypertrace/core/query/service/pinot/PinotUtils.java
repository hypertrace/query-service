package org.hypertrace.core.query.service.pinot;

public class PinotUtils {

  public static String getZkPath(String zkBasePath, String pinotClusterName) {
    return zkBasePath.endsWith("/")
        ? zkBasePath + pinotClusterName
        : zkBasePath + "/" + pinotClusterName;
  }
}

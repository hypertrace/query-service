package org.hypertrace.core.query.service.pinot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;

/*
 * Factory to create PinotClient based on given zookeeper path.
 */
@Slf4j
public class PinotClientFactory {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(PinotClientFactory.class);
  // Singleton instance
  private static final PinotClientFactory INSTANCE = new PinotClientFactory();

  private final ConcurrentHashMap<String, PinotClient> clientMap = new ConcurrentHashMap<>();

  private PinotClientFactory() {}

  // Create a Pinot Client.
  public static PinotClient createPinotClient(String pinotCluster, String pathType, String path) {
    if (!get().containsClient(pinotCluster)) {
      synchronized (get()) {
        if (!get().containsClient(pinotCluster)) {
          get().addPinotClient(pinotCluster, new PinotClient(pathType, path));
        }
      }
    }
    return get().getPinotClient(pinotCluster);
  }

  public static PinotClientFactory get() {
    return INSTANCE;
  }

  private void addPinotClient(String cluster, PinotClient client) {
    this.clientMap.put(cluster, client);
  }

  public boolean containsClient(String clusterName) {
    return this.clientMap.containsKey(clusterName);
  }

  public PinotClient getPinotClient(String clusterName) {
    return this.clientMap.get(clusterName);
  }

  public static class PinotClient {

    private static final String SQL_FORMAT = "sql";

    private final Connection connection;
    private final RateLimiter rateLimiter = RateLimiter.create(1 / 60d);

    @VisibleForTesting
    public PinotClient(Connection connection) {
      this.connection = connection;
    }

    private PinotClient(String pathType, String path) {
      switch (pathType.toLowerCase()) {
        case "zk":
        case "zookeeper":
          LOG.info("Trying to create a Pinot client connected to Zookeeper: {}", path);
          this.connection = ConnectionFactory.fromZookeeper(path);
          break;
        case "broker":
          LOG.info("Trying to create a Pinot client with default brokerlist: {}", path);
          this.connection = ConnectionFactory.fromHostList(path);
          break;
        default:
          throw new RuntimeException("Unsupported Pinot Client scheme: " + pathType);
      }
    }

    public ResultSetGroup executeQuery(String statement, Params params) {
      /*
      PreparedStatement preparedStatement = buildPreparedStatement(statement, params);
      return preparedStatement.execute();
      */
      try {
        return connection.execute(new Request(SQL_FORMAT, resolveStatement(statement, params)));
      } catch (Exception ex) {
        if (rateLimiter.tryAcquire()) {
          log.info(
              "Error while handling the request - statement: {}, params: {}",
              statement,
              params,
              ex);
        } else if (log.isDebugEnabled()) {
          log.debug(
              "Error while handling the request - statement: {}, params: {}",
              statement,
              params,
              ex);
        }
        throw ex;
      }
    }

    private PreparedStatement buildPreparedStatement(String statement, Params params) {
      Request request = new Request(SQL_FORMAT, statement);
      PreparedStatement preparedStatement = connection.prepareStatement(request);
      params.getStringParams().forEach(preparedStatement::setString);
      params.getIntegerParams().forEach(preparedStatement::setInt);
      params.getLongParams().forEach(preparedStatement::setLong);
      params.getDoubleParams().forEach(preparedStatement::setDouble);
      params.getFloatParams().forEach(preparedStatement::setFloat);
      params
          .getByteStringParams()
          .forEach((i, b) -> preparedStatement.setString(i, Hex.encodeHexString(b.toByteArray())));
      return preparedStatement;
    }

    @VisibleForTesting
    /*
     * Pinot PreparedStatement creates invalid query if one of the parameters has '?' in its value.
     * Sample pinot query : select * from table where team in (?, ?, ?).. Now say parameters are:
     * 'abc', 'pqr with (?)' and 'xyz'..
     *
     * Now, on executing PreparedStatement:fillStatementWithParameters method on this will return
     * select * from table where team in ('abc', 'pqr with ('xyz')', ?) -- which is clearly wrong
     * (what we wanted was select * from table where team in ('abc', 'pqr with (?)', 'xyz'))..
     *
     * The reason is the usage of replaceFirst iteration in the pinot PreparedStatement method..
     *
     * Hence written this custom method to resolve the query statement rather than relying on Pinot's
     * library method.
     * This is a temporary fix and can be reverted when the Pinot issue gets resolved
     * Raised an issue in incubator-pinot github repo: apache/incubator-pinot#6834
     */
    static String resolveStatement(String query, Params params) {
      if (query.isEmpty()) {
        return query;
      }
      String[] queryParts = query.split("\\?");

      String[] parameters = new String[queryParts.length];
      params.getStringParams().forEach((i, p) -> parameters[i] = getStringParam(p));
      params.getIntegerParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getLongParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getDoubleParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getFloatParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params
          .getByteStringParams()
          .forEach((i, p) -> parameters[i] = getStringParam(Hex.encodeHexString(p.toByteArray())));

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < queryParts.length; i++) {
        sb.append(queryParts[i]);
        sb.append(parameters[i] != null ? parameters[i] : "");
      }
      String statement = sb.toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resolved PQL statement: [{}]", statement);
      }
      return statement;
    }

    private static String getStringParam(String value) {
      return "'" + value.replace("'", "''") + "'";
    }
  }
}

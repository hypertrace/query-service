package org.hypertrace.core.query.service.postgres;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discovers the map attributes indexes from Postgres Result Set */
class PostgresResultAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresResultAnalyzer.class);

  /* Stores the Non-Map Attributes logical name to Physical Name index */
  private final Map<String, Integer> logicalNameToPhysicalNameIndex;
  private final ResultSet resultSet;
  private final TableDefinition tableDefinition;
  private final Map<String, RateLimiter> attributeLogRateLimitter;
  private final PostgresMapConverter postgresMapConverter;

  PostgresResultAnalyzer(
      ResultSet resultSet,
      LinkedHashSet<String> selectedAttributes,
      TableDefinition tableDefinition,
      Map<String, Integer> logicalNameToPhysicalNameIndex) {
    this.logicalNameToPhysicalNameIndex = logicalNameToPhysicalNameIndex;
    this.resultSet = resultSet;
    this.tableDefinition = tableDefinition;
    this.attributeLogRateLimitter = new HashMap<>();
    selectedAttributes.forEach(e -> attributeLogRateLimitter.put(e, RateLimiter.create(0.5)));
    this.postgresMapConverter = new PostgresMapConverter();
  }

  /** For each selected attributes build the map of logical name to result index. */
  static PostgresResultAnalyzer create(
      ResultSet resultSet,
      LinkedHashSet<String> selectedAttributes,
      TableDefinition tableDefinition)
      throws SQLException {
    Map<String, Integer> logicalNameToPhysicalNameIndex = new HashMap<>();

    for (String logicalName : selectedAttributes) {
      String name = tableDefinition.getPhysicalColumnName(logicalName);
      ResultSetMetaData metaData = resultSet.getMetaData();
      for (int colIndex = 0; colIndex < metaData.getColumnCount(); colIndex++) {
        String physName = metaData.getColumnName(colIndex);
        if (physName.equalsIgnoreCase(name)) {
          logicalNameToPhysicalNameIndex.put(logicalName, colIndex);
          break;
        }
      }
    }
    LOG.info("Attributes to Index: {}", logicalNameToPhysicalNameIndex);
    return new PostgresResultAnalyzer(
        resultSet, selectedAttributes, tableDefinition, logicalNameToPhysicalNameIndex);
  }

  @VisibleForTesting
  Integer getPhysicalColumnIndex(String logicalName) {
    return logicalNameToPhysicalNameIndex.get(logicalName);
  }

  /**
   * Gets the data from Result Set Row, will never null
   *
   * @throws IllegalStateException if index is missing for merging or there's an issue with the data
   *     format in Postgres
   * @return merged map data if in correct format. Will never return null
   */
  @Nonnull
  String getDataFromRow(String logicalName) throws SQLException {
    Integer colIndex = getPhysicalColumnIndex(logicalName);
    return resultSet.getString(colIndex);
  }
}

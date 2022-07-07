package org.hypertrace.core.query.service.postgres;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discovers the map attributes indexes from Postgres Result Set */
class PostgresResultAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresResultAnalyzer.class);

  /* Stores the Map Attributes logical name to Physical Names */
  private final Map<String, Integer> mapLogicalNameToKeyIndex;
  private final Map<String, Integer> mapLogicalNameToValueIndex;

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
      Map<String, Integer> mapLogicalNameToKeyIndex,
      Map<String, Integer> mapLogicalNameToValueIndex,
      Map<String, Integer> logicalNameToPhysicalNameIndex) {
    this.mapLogicalNameToKeyIndex = mapLogicalNameToKeyIndex;
    this.mapLogicalNameToValueIndex = mapLogicalNameToValueIndex;
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
    Map<String, Integer> mapLogicalNameToKeyIndex = new HashMap<>();
    Map<String, Integer> mapLogicalNameToValueIndex = new HashMap<>();
    Map<String, Integer> logicalNameToPhysicalNameIndex = new HashMap<>();

    for (String logicalName : selectedAttributes) {
      if (tableDefinition.isMap(logicalName)) {
        String keyPhysicalName = tableDefinition.getKeyColumnNameForMap(logicalName);
        String valuePhysicalName = tableDefinition.getValueColumnNameForMap(logicalName);
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int colIndex = 0; colIndex < metaData.getColumnCount(); colIndex++) {
          String physName = metaData.getColumnName(colIndex);
          if (physName.equalsIgnoreCase(keyPhysicalName)) {
            mapLogicalNameToKeyIndex.put(logicalName, colIndex);
          } else if (physName.equalsIgnoreCase(valuePhysicalName)) {
            mapLogicalNameToValueIndex.put(logicalName, colIndex);
          }
        }
      } else {
        List<String> names = tableDefinition.getPhysicalColumnNames(logicalName);
        Preconditions.checkArgument(names.size() == 1);
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int colIndex = 0; colIndex < metaData.getColumnCount(); colIndex++) {
          String physName = metaData.getColumnName(colIndex);
          if (physName.equalsIgnoreCase(names.get(0))) {
            logicalNameToPhysicalNameIndex.put(logicalName, colIndex);
            break;
          }
        }
      }
    }
    LOG.info("Map LogicalName to Key Index: {} ", mapLogicalNameToKeyIndex);
    LOG.info("Map LogicalName to Value Index: {}", mapLogicalNameToValueIndex);
    LOG.info("Attributes to Index: {}", logicalNameToPhysicalNameIndex);
    return new PostgresResultAnalyzer(
        resultSet,
        selectedAttributes,
        tableDefinition,
        mapLogicalNameToKeyIndex,
        mapLogicalNameToValueIndex,
        logicalNameToPhysicalNameIndex);
  }

  @VisibleForTesting
  Integer getMapKeyIndex(String logicalName) {
    return mapLogicalNameToKeyIndex.get(logicalName);
  }

  @VisibleForTesting
  Integer getMapValueIndex(String logicalName) {
    return mapLogicalNameToValueIndex.get(logicalName);
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
    String result;
    if (tableDefinition.isMap(logicalName)) {
      Integer keyIndex = getMapKeyIndex(logicalName);
      if (keyIndex == null) {
        LOG.info("Map LogicalName to Key Index: {} ", mapLogicalNameToKeyIndex);
        LOG.info("Attributes to Index: {}", logicalNameToPhysicalNameIndex);
        throw new IllegalStateException(
            "Unable to find the key index to attribute: " + logicalName);
      }
      String keyData = resultSet.getString(keyIndex);

      String valueData = "";
      Integer valueIndex = getMapValueIndex(logicalName);
      if (valueIndex == null) {
        if (attributeLogRateLimitter.get(logicalName).tryAcquire()) {
          LOG.error("Unable to find the map value column index for Attribute: {}.", logicalName);
          LOG.info("Map LogicalName to Value Index: {} ", mapLogicalNameToValueIndex);
          LOG.info("Attributes to Index: {}", logicalNameToPhysicalNameIndex);
        }
      } else {
        valueData = resultSet.getString(valueIndex);
      }
      try {
        result = postgresMapConverter.merge(keyData, valueData);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Unable to merge the map data for attribute " + logicalName, e);
      }
    } else {
      Integer colIndex = getPhysicalColumnIndex(logicalName);
      result = resultSet.getString(colIndex);
    }
    return result;
  }
}

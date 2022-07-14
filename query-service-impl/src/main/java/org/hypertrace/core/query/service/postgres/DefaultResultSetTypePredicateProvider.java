package org.hypertrace.core.query.service.postgres;

import java.sql.ResultSet;

public class DefaultResultSetTypePredicateProvider implements ResultSetTypePredicateProvider {
  @Override
  public boolean isSelectionResultSetType(ResultSet resultSet) {
    return resultSet.getClass().getName().contains("SelectionResultSet");
  }

  @Override
  public boolean isResultTableResultSetType(ResultSet resultSet) {
    return resultSet.getClass().getName().contains("ResultTableResultSet");
  }
}

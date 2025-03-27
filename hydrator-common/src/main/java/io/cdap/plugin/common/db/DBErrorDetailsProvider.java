/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common.db;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A custom ErrorDetailsProvider for Database plugins.
 */
public class DBErrorDetailsProvider implements ErrorDetailsProvider {

  private static final Map<String, ErrorType> ERROR_CODE_TO_ERROR_TYPE;
  private static final Map<String, ErrorCategory> ERROR_CODE_TO_ERROR_CATEGORY;
  private static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. Error message: %s";
  private static final String DEFAULT_EXTERNAL_DOCUMENTATION_LINK = "https://en.wikipedia.org/wiki/SQLSTATE";

  static {
    // https://en.wikipedia.org/wiki/SQLSTATE
    ERROR_CODE_TO_ERROR_TYPE = new HashMap<>();
    ERROR_CODE_TO_ERROR_TYPE.put("01", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("02", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("07", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("08", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("09", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0A", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("0D", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0E", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0F", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0K", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0L", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0M", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0N", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0P", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0S", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0T", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0U", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0V", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0W", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0X", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0Y", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("0Z", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("10", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("20", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("21", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("22", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("23", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("24", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("25", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("26", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("27", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("28", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("2B", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("2C", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("2D", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("2E", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("2F", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("2H", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("30", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("33", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("34", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("35", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("36", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("38", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("39", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("3B", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("3C", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("3D", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("3F", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("40", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("42", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("44", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("45", ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put("46", ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put("HW", ErrorType.SYSTEM);

    ERROR_CODE_TO_ERROR_CATEGORY = new HashMap<>();
    ErrorCategory.ErrorCategoryEnum plugin = ErrorCategory.ErrorCategoryEnum.PLUGIN;
    ERROR_CODE_TO_ERROR_CATEGORY.put("01", new ErrorCategory(plugin, "DB Warning"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("02", new ErrorCategory(plugin, "DB No Data"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("07", new ErrorCategory(plugin, "DB Dynamic SQL error"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("08", new ErrorCategory(plugin, "DB Connection Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("09", new ErrorCategory(plugin, "DB Triggered Action Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0A", new ErrorCategory(plugin, "DB Feature Not Supported"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0D", new ErrorCategory(plugin, "DB Invalid Target Type Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0E", new ErrorCategory(plugin, "DB Invalid Schema Name List Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0F", new ErrorCategory(plugin, "DB Locator Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0K", new ErrorCategory(plugin, "DB Resignal When Handler Not Active"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0L", new ErrorCategory(plugin, "DB Invalid Grantor"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0M", new ErrorCategory(plugin, "DB Invalid SQL-Invoked Procedure Reference"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0N", new ErrorCategory(plugin, "DB SQL/XML Mapping Error"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0P", new ErrorCategory(plugin, "DB Invalid Role Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0S", new ErrorCategory(plugin, "DB Invalid Transform Group Name Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0T",
      new ErrorCategory(plugin, "DB Target Table Disagrees With Cursor Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0U", new ErrorCategory(plugin, "DB Attempt To Assign To Non-Updatable Column"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0V", new ErrorCategory(plugin, "DB Attempt To Assign To Ordering Column"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0W", new ErrorCategory(plugin, "DB Prohibited Statement Encountered"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0X", new ErrorCategory(plugin, "DB Invalid Foreign Server Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0Y", new ErrorCategory(plugin, "DB Pass-Through Specific Condition"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("0Z", new ErrorCategory(plugin, "DB Diagnostics Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("10", new ErrorCategory(plugin, "DB XQuery Error"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("20", new ErrorCategory(plugin, "DB Case Not Found For Case Statement"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("21", new ErrorCategory(plugin, "DB Cardinality Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("22", new ErrorCategory(plugin, "DB Data Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("23", new ErrorCategory(plugin, "DB Integrity Constraint Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("24", new ErrorCategory(plugin, "DB Invalid Cursor State"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("25", new ErrorCategory(plugin, "DB Invalid Transaction State"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("26", new ErrorCategory(plugin, "DB Invalid SQL Statement Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("27", new ErrorCategory(plugin, "DB Triggered Data Change Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("28", new ErrorCategory(plugin, "DB Invalid Authorization Specification"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2B", new ErrorCategory(plugin, "DB Dependent Privilege Descriptors Still Exist"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2C", new ErrorCategory(plugin, "DB Invalid Character Set Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2D", new ErrorCategory(plugin, "DB Invalid Transaction Termination"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2E", new ErrorCategory(plugin, "DB Invalid Connection Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2F", new ErrorCategory(plugin, "DB SQL Routine Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("2H", new ErrorCategory(plugin, "DB Invalid Collation Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("30", new ErrorCategory(plugin, "DB Invalid SQL Statement Identifier"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("33", new ErrorCategory(plugin, "DB Invalid SQL Descriptor Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("34", new ErrorCategory(plugin, "DB Invalid Cursor Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("35", new ErrorCategory(plugin, "DB Invalid Condition Number"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("36", new ErrorCategory(plugin, "DB Cursor Sensitivity Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("38", new ErrorCategory(plugin, "DB External Routine Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("39", new ErrorCategory(plugin, "DB External Routine Invocation Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("3B", new ErrorCategory(plugin, "DB Savepoint Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("3C", new ErrorCategory(plugin, "DB Ambiguous Cursor Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("3D", new ErrorCategory(plugin, "DB Invalid Catalog Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("3F", new ErrorCategory(plugin, "DB Invalid Schema Name"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("40", new ErrorCategory(plugin, "DB Transaction Rollback"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("42", new ErrorCategory(plugin, "DB Syntax Error or Access Rule Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("44", new ErrorCategory(plugin, "DB With Check Option Violation"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("45", new ErrorCategory(plugin, "DB Unhandled User-Defined Exception"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("46", new ErrorCategory(plugin, "DB JAVA DDL"));
    ERROR_CODE_TO_ERROR_CATEGORY.put("HW", new ErrorCategory(plugin, "DB Datalink Exception"));
  }

  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof SQLException) {
        return getProgramFailureException((SQLException) t, errorContext);
      }
      if (t instanceof IllegalArgumentException) {
        return getProgramFailureException((IllegalArgumentException) t, errorContext, ErrorType.USER);
      }
      if (t instanceof IllegalStateException || t instanceof InstantiationException) {
        return getProgramFailureException((Exception) t, errorContext, ErrorType.SYSTEM);
      }
    }
    return null;
  }

  public ProgramFailureException getProgramFailureException(SQLException e, @Nullable ErrorContext errorContext) {
    String errorMessage =
      String.format("SQL Exception occurred: [Message='%s', SQLState='%s', ErrorCode='%s'].", e.getMessage(),
        e.getSQLState(), e.getErrorCode());
    String sqlState = e.getSQLState();
    int errorCode = e.getErrorCode();
    String errorMessageWithDetails = errorContext != null ?
      String.format("Error occurred in the phase: '%s' with sqlState: '%s', errorCode: '%s', errorMessage: %s",
        errorContext.getPhase(), sqlState, errorCode, errorMessage) :
      String.format("SQL Error occurred, sqlState: '%s', errorCode: '%s', errorMessage: %s", sqlState, errorCode,
        errorMessage);
    String externalDocumentationLink = getExternalDocumentationLink();
    if (!Strings.isNullOrEmpty(externalDocumentationLink)) {
      if (!errorMessage.endsWith(".")) {
        errorMessage = errorMessage + ".";
      }
      errorMessage = String.format("%s For more details, see %s", errorMessage, externalDocumentationLink);
    }
    return ErrorUtils.getProgramFailureException(
      Strings.isNullOrEmpty(sqlState) ? new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN) :
        getErrorCategoryFromSqlState(sqlState), errorMessage, errorMessageWithDetails,
      getErrorTypeFromErrorCodeAndSqlState(errorCode, sqlState), true, ErrorCodeType.SQLSTATE, sqlState,
      externalDocumentationLink, e);
  }

  private ProgramFailureException getProgramFailureException(Exception e, ErrorContext errorContext,
                                                             ErrorType errorType) {
    String errorMessage = e.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage, String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(), errorMessage), errorType, false, e);
  }

  /**
   * Get the external documentation link for the client errors if available.
   *
   * @return The external documentation link as a {@link String}.
   */
  protected String getExternalDocumentationLink() {
    return DEFAULT_EXTERNAL_DOCUMENTATION_LINK;
  }

  /**
   * Get the {@link ErrorType} for the given error code and SQL state.
   * Override this method to provide custom error types based on the error code and SQL state.
   *
   * @param errorCode The error code.
   * @param sqlState  The SQL state.
   * @return The {@link ErrorType} for the given error code and SQL state.
   */
  protected ErrorType getErrorTypeFromErrorCodeAndSqlState(int errorCode, String sqlState) {
    if (!Strings.isNullOrEmpty(sqlState) && sqlState.length() >= 2 &&
      ERROR_CODE_TO_ERROR_TYPE.containsKey(sqlState.substring(0, 2))) {
      return ERROR_CODE_TO_ERROR_TYPE.get(sqlState.substring(0, 2));
    }
    return ErrorType.UNKNOWN;
  }

  /**
   * Get the {@link ErrorCategory} for the given SQL state.
   * Implements generic error categories based on the SQL state.
   * See <a href="https://en.wikipedia.org/wiki/SQLSTATE">SQLSTATE</a> for more information.
   * Override this method to provide custom error categories based on the SQL state.
   *
   * @param sqlState The SQL state.
   * @return The {@link ErrorCategory} for the given SQL state.
   */
  protected ErrorCategory getErrorCategoryFromSqlState(String sqlState) {
    if (!Strings.isNullOrEmpty(sqlState) && sqlState.length() >= 2 &&
      ERROR_CODE_TO_ERROR_CATEGORY.containsKey(sqlState.substring(0, 2))) {
      return ERROR_CODE_TO_ERROR_CATEGORY.get(sqlState.substring(0, 2));
    }
    return new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN);
  }
}

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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.plugin.common.HydratorErrorDetailsProvider;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DiskChecker;

import java.io.FileNotFoundException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.security.auth.login.FailedLoginException;

/**
 * FileErrorDetails provider
 */
public class FileErrorDetailsProvider extends HydratorErrorDetailsProvider {
 private static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. %s: %s";

  private static final Map<Class<? extends Throwable>, ErrorType> exceptionErrorTypeMap =
    new ImmutableMap.Builder<Class<? extends Throwable>, ErrorType>()
      .put(FileNotFoundException.class, ErrorType.USER)
      .put(AccessControlException.class, ErrorType.USER)
      .put(ParentNotDirectoryException.class, ErrorType.USER)
      .put(InvalidPathException.class, ErrorType.USER)
      .put(FileAlreadyExistsException.class, ErrorType.USER)
      .put(QuotaExceededException.class, ErrorType.USER)
      .put(PathIsNotDirectoryException.class, ErrorType.USER)
      .put(InvalidRequestException.class, ErrorType.USER)
      .put(ChecksumException.class, ErrorType.USER)
      .put(RemoteException.class, ErrorType.SYSTEM)
      .put(SocketTimeoutException.class, ErrorType.SYSTEM)
      .put(DiskChecker.DiskOutOfSpaceException.class, ErrorType.SYSTEM)
      .put(StandbyException.class, ErrorType.SYSTEM)
      .put(NoRouteToHostException.class, ErrorType.SYSTEM)
      .put(BlockMissingException.class, ErrorType.SYSTEM)
      .put(ReplicaNotFoundException.class, ErrorType.SYSTEM)
      .put(InvalidBlockTokenException.class, ErrorType.SYSTEM)
      .put(SafeModeException.class, ErrorType.SYSTEM)
      .put(TimeoutException.class, ErrorType.SYSTEM)
      .put(FailedLoginException.class, ErrorType.SYSTEM)
      .put(MetadataException.class, ErrorType.SYSTEM)
      .build();

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    // Call super method to get base exception details
    ProgramFailureException ex = super.getExceptionDetails(e, errorContext);
    if (ex != null) {
      return ex;
    }
    return getFileBasedExceptionDetails(e, null, errorContext);
  }

  private static ProgramFailureException getFileBasedExceptionDetails(Exception e, @Nullable String errorReason,
                                                                      @Nullable ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);

    for (Throwable t : causalChain) {
      for (Map.Entry<Class<? extends Throwable>, ErrorType> entry : exceptionErrorTypeMap.entrySet()) {
        if (entry.getKey().isInstance(t)) {
          return getProgramFailureException((Exception) t, errorContext, entry.getValue(), errorReason, true);
        }
      }
    }
    return null;
  }

  /**
   * Retrieves detailed exception information for file-based errors from an exception chain.
   *
   * @param e The Exception to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  public static ProgramFailureException getFileBasedProgramFailureExceptionDetailsFromChain(Exception e,
                                                                                     @Nullable String errorReason) {
    ProgramFailureException ex = getFileBasedExceptionDetails(e, errorReason, null);
    if (ex == null) {
     return getProgramFailureException(e, null, ErrorType.UNKNOWN, errorReason, false);
    }
    return ex;
  }

  /**
   * Get a ProgramFailureException with the given error information from {@link Exception}.
   *
   * @param e The Exception to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private static ProgramFailureException getProgramFailureException(Exception e, @Nullable ErrorContext errorContext,
                                         ErrorType errorType, @Nullable String errorReason, boolean dependency) {
    if (Strings.isNullOrEmpty(errorReason)) {
      errorReason = e.getMessage();
    }
    String errorMessage = e.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
    errorReason, errorContext != null ? String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(), e.getClass()
  .getName(), errorMessage) : String.format("%s: %s", e.getClass().getName(), errorMessage), errorType, dependency, e);
  }
}

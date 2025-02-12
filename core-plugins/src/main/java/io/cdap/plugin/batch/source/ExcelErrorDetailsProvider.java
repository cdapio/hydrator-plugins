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

import com.github.pjfanning.xlsx.exceptions.MissingSheetException;
import com.github.pjfanning.xlsx.exceptions.ReadException;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import org.apache.poi.EmptyFileException;

import java.util.List;
import javax.annotation.Nullable;

/**
 * ExcelErrorDetailsProvider provider
 */
public class ExcelErrorDetailsProvider implements ErrorDetailsProvider {

    private static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. Error message: %s";
    private static final String SUBCATEGORY_CONFIGURATION = "Configuration";
    private static final String SUBCATEGORY_DATA_MISSING = "Data Integrity";
    private static final String SUBCATEGORY_FILE_READ_ERROR = "File Read";

    @Nullable
    @Override
    public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
        List<Throwable> causalChain = Throwables.getCausalChain(e);
        for (Throwable t : causalChain) {
            if (t instanceof ProgramFailureException) {
                // if causal chain already has program failure exception, return null to avoid double wrap.
                return null;
            }
            if (t instanceof MissingSheetException) {
                return getProgramFailureException((MissingSheetException) t, errorContext,
                        ErrorType.USER, SUBCATEGORY_DATA_MISSING);
            }
            if (t instanceof ReadException) {
                return getProgramFailureException((ReadException) t, errorContext,
                        ErrorType.USER, SUBCATEGORY_FILE_READ_ERROR);
            }
            if (t instanceof EmptyFileException) {
                return getProgramFailureException((EmptyFileException) t, errorContext,
                        ErrorType.USER, SUBCATEGORY_DATA_MISSING);
            }
            if (t instanceof IllegalArgumentException) {
                return getProgramFailureException((IllegalArgumentException) t, errorContext,
                        ErrorType.USER, SUBCATEGORY_CONFIGURATION);
            }
        }
        return null;
    }

    /**
     * Get a ProgramFailureException with the given error information from {@link Exception}.
     *
     * @param exception The Exception to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(Exception exception, ErrorContext errorContext,
                                                               ErrorType errorType, String subCategory) {
        String errorMessage = exception.getMessage();
        return ErrorUtils.getProgramFailureException(
                new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN, subCategory), errorMessage,
                String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(), errorMessage), errorType,
                false, exception);
    }
}

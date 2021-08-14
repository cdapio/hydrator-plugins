/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.common.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Exception Utility to handle exception easily
 * Not using the one in common-lang , because it's test dependency and including it in runtime may cause some issue.
 */
public final class ExceptionUtils {
  private static final String[] CAUSE_METHOD_NAMES = new String[]{"getCause", "getNextException", "getTargetException"
    , "getException", "getSourceException", "getRootCause", "getCausedByException", "getNested", "getLinkedException"
    , "getNestedException", "getLinkedCause", "getThrowable"};

  private ExceptionUtils() {
  }

  private static Throwable getCause(Throwable throwable) {
    return getCause(throwable, (String[]) null);
  }

  private static Throwable getCause(Throwable throwable, String[] methodNames) {
    if (throwable == null) {
      return null;
    } else {
      if (methodNames == null) {
        Throwable cause = throwable.getCause();
        if (cause != null) {
          return cause;
        }

        methodNames = CAUSE_METHOD_NAMES;
      }

      String[] var7 = methodNames;
      int var3 = methodNames.length;

      for (int var4 = 0; var4 < var3; ++var4) {
        String methodName = var7[var4];
        if (methodName != null) {
          Throwable legacyCause = getCauseUsingMethodName(throwable, methodName);
          if (legacyCause != null) {
            return legacyCause;
          }
        }
      }

      return null;
    }
  }

  /**
   * Introspects the Throwable to obtain the root cause.
   * This method walks through the exception chain to the last element, "root" of the tree, using getCause(Throwable)
   * and returns that exception.
   * If the throwable parameter has a cause of itself, then null will be returned. If the throwable parameter cause
   * chain loops, the last element in the chain before the loop is returned.
   * Params:
   * @param throwable the throwable to get the root cause for, may be null
   * @return the root cause of the Throwable, null if none found or null throwable input
   */
  public static Throwable getRootCause(Throwable throwable) {
    List<Throwable> list = getThrowableList(throwable);
    return list.size() < 2 ? null : (Throwable) list.get(list.size() - 1);
  }

  private static Throwable getCauseUsingMethodName(Throwable throwable, String methodName) {
    Method method = null;

    try {
      method = throwable.getClass().getMethod(methodName);
    } catch (NoSuchMethodException var7) {
    } catch (SecurityException var8) {
    }

    if (method != null && Throwable.class.isAssignableFrom(method.getReturnType())) {
      try {
        return (Throwable) method.invoke(throwable);
      } catch (IllegalAccessException var4) {
      } catch (IllegalArgumentException var5) {
      } catch (InvocationTargetException var6) {
      }
    }

    return null;
  }

  /**
   * Returns the list of Throwable objects in the exception chain.
   * A throwable without cause will return a list containing one element - the input throwable. A throwable with one
   * cause will return a list containing two elements. - the input throwable and the cause throwable. A null
   * throwable will return a list of size zero.
   * This method handles recursive cause structures that might otherwise cause infinite loops. The cause chain is
   * processed until the end is reached, or until the next item in the chain is already in the result set.
   * @param throwable the throwable to inspect, may be null
   * @return the list of throwables, never null
   */
  public static List<Throwable> getThrowableList(Throwable throwable) {
    ArrayList list;
    for (list = new ArrayList(); throwable != null && !list.contains(throwable); throwable = getCause(throwable)) {
      list.add(throwable);
    }

    return list;
  }

  /**
   * Gets a short message summarising the exception.
   * The message returned is of the form {ClassNameWithoutPackage}: {ThrowableMessage}
   * @param th the throwable to get a message for, null returns empty string
   * @return the message, non-null
   */
  public static String getMessage(Throwable th) {
    if (th == null) {
      return "";
    } else {
      String clsName = th.getClass().getSimpleName();
      String msg = th.getMessage();
      return clsName + ": " + msg;
    }
  }

  /**
   * Gets a short message summarising the root cause exception.
   * The message returned is of the form {ClassNameWithoutPackage}: {ThrowableMessage}
   * @param th the throwable to get a message for, null returns empty string
   * @return the message, non-null
   */
  public static String getRootCauseMessage(Throwable th) {
    Throwable root = getRootCause(th);
    root = root == null ? th : root;
    return getMessage(root);
  }
}

/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A package private class for executing an external program.
 */
final class RunExternalProgramExecutor extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(Run.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final String executable;
  private final BlockingQueue<Event> eventQueue;
  private ExecutorService executor;
  private Process process;
  private Thread shutdownThread;
  private List<String> outputList = new ArrayList<>();

  RunExternalProgramExecutor(String executable) {
    this.executable = executable;
    this.eventQueue = new LinkedBlockingQueue<Event>();
  }

  @Override
  protected void startUp() throws Exception {
    // We need two threads.
    // One thread for keep reading from input, write to process stdout and read from stdin.
    // The other for keep reading stderr and log.
    executor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setDaemon(true).build());

    // The Shutdown thread is to time the shutdown and kill the process if it timeout.
    shutdownThread = createShutdownThread();

    process = Runtime.getRuntime().exec(executable);
    executor.execute(createProcessRunnable(process));
    executor.execute(createLogRunnable(process));
  }

  @Override
  protected void run() throws Exception {
    // Simply wait for the process to complete.
    // Trigger shutdown would trigger closing of in/out streams of the process,
    // which if the process implemented correctly, should complete.
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalArgumentException(String.format("Process exit with exit code '%s'.", exitCode));
    }
  }

  @Override
  protected void triggerShutdown() {
    executor.shutdownNow();
    shutdownThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    shutdownThread.interrupt();
    shutdownThread.join();

    // Need to notify all pending events as failure.
    List<Event> events = Lists.newLinkedList();
    eventQueue.drainTo(events);
    for (Event event : events) {
      event.getCompletion().setException(
        new IllegalStateException("External program already stopped."));
    }
  }

  /**
   * Sends input to the executable threads, and emits the output structured records.
   *
   * @param line - Space separated sequence of the inputs that will be passed as STDIN to the executable binary.
   * @param emitter
   * @param structuredRecord
   * @param outputSchema
   */
  void submit(String line, Emitter<StructuredRecord> emitter, StructuredRecord structuredRecord, Schema outputSchema) {
    SettableFuture<String> completion = SettableFuture.create();
    try {
      eventQueue.put(new Event(line, completion));
      Futures.successfulAsList(completion).get();

      // Read the output and emit the structured record.
      for (String output : outputList) {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputSchema.getFields()) {
          if (structuredRecord.getSchema().getField(field.getName()) != null) {
            builder.set(field.getName(), structuredRecord.get(field.getName()));
          } else {
            if (field.getSchema().getType().equals(Schema.Type.STRING)) {
              builder.set(field.getName(), output);
            } else {
              builder.convertAndSet(field.getName(), output);
            }
          }
        }
        emitter.emit(builder.build());
        outputList.clear();
      }
    } catch (Exception e) {
      completion.setException(e);
    }
  }

  /**
   * Creates and returns the shutdown thread.
   *
   * @return shutdown thread
   */
  private Thread createShutdownThread() {
    Thread t = new Thread() {
      @Override
      public void run() {
        // Wait for at most SHUTDOWN_TIME and kill the process.
        try {
          TimeUnit.SECONDS.sleep(SHUTDOWN_TIMEOUT_SECONDS);
          process.destroy();
        } catch (InterruptedException e) {
          // If interrupted, means the process has been shutdown nicely.
        }
      }
    };
    t.setDaemon(true);
    return t;
  }

  /**
   * Reads the standard output.
   *
   * @param process
   * @return runnable object
   */
  private Runnable createProcessRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.UTF_8));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(process.getOutputStream(), Charsets.UTF_8), true);

        try {
          while (!Thread.currentThread().isInterrupted()) {
            Event event = eventQueue.take();
            try {
              writer.println(event.getLine());
              String line = reader.readLine();
              outputList.add(line);
              event.getCompletion().set(line);
            } catch (IOException e) {
              throw new IllegalArgumentException("Exception while reading the standard output from stream.", e);
            }
          }
        } catch (InterruptedException e) {
          // No-op. It's just for signal stopping of the thread.
        } finally {
          Closeables.closeQuietly(writer);
          Closeables.closeQuietly(reader);
        }
      }
    };
  }

  /**
   * Reads the standard error.
   *
   * @param process
   * @return runnable object
   */
  private Runnable createLogRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), Charsets.UTF_8));
        try {
          String line = reader.readLine();
          while (!Thread.currentThread().isInterrupted() && line != null) {
            // Write to the CDAP logs.
            LOG.error(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          throw new IllegalArgumentException("Exception while reading the standard error from stream.", e);
        } finally {
          Closeables.closeQuietly(reader);
        }
      }
    };
  }

  /**
   * Class to maintain the input/output information.
   */
  private static final class Event {
    private final String line;
    private final SettableFuture<String> completion;

    Event(String line, SettableFuture<String> completion) {
      this.line = line;
      this.completion = completion;
    }

    private String getLine() {
      return line;
    }

    private SettableFuture<String> getCompletion() {
      return completion;
    }
  }
}

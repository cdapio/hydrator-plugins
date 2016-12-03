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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

/**
 * Transforms records using custom java code provided by the config.
 */
@Plugin(type = "transform")
@Name("Java")
@Description("Executes user provided java code on each record.")
public class JavaTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(JavaTransform.class);
  private static final JavaCompiler javac = ToolProvider.getSystemJavaCompiler();

  private final Config config;

  private Schema schema;
  private Method method;
  private Object userObject;

  /**
   * Configuration for the java transform.
   */
  public static class Config extends PluginConfig {
    @Description("Java code defining how to transform input record into zero or more records. " +
      "The code must implement a method " +
      "called 'transform', which takes as input as stringified JSON object (representing the input record) " +
      "emitter object, which can be used to emit records and error messages" +
      "and a context object (which contains CDAP metrics, logger and lookup)" +
      "For example:\n" +
      // TODO example
      "'public void transform(StructuredRecord input, Emitter<StructureRecord> emitter) {\n" +
      "}'\n" +
      "will emit an error if the input id is present in blacklist table, else scale the 'count' field by 1024"
    )
    private final String code;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    // TODO: leverage the schema

    public Config(String code, String schema) {
      this.code = code;
      this.schema = schema;
    }
  }

  JavaTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    try {
      if (config.schema != null) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(parseJson(config.schema));
      } else {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
      }

      init();
    } catch (Exception e) {
      throw new RuntimeException("Error compiling the code", e);
    }
  }

  private Schema parseJson(String schema) {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
    }
  }


  // test out: two java transforms in the same pipeline

  private void init() throws Exception {
    Class<?> classz = compile("CustomTransform", config.code);
    // ensure that it has such a method
    method = classz.getDeclaredMethod("transform", StructuredRecord.class, Emitter.class);
    userObject = classz.newInstance();
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if (config.schema != null) {
      schema = parseJson(config.schema);
    }
    init();
  }

  // remove script filter transform (or deprecate it)

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    method.invoke(userObject, structuredRecord, emitter);
  }

  private Class<?> compile(String className, String sourceCodeText) throws Exception {
    SourceCode sourceCode = new SourceCode(className, sourceCodeText);
    CompiledCode compiledCode = new CompiledCode(className);
    Iterable<? extends JavaFileObject> compilationUnits = Collections.singletonList(sourceCode);
    DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(this.getClass().getClassLoader());
    dynamicClassLoader.setCode(compiledCode);
    DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();
    // TODO: pass in Writer for first param to javac
    ExtendedStandardJavaFileManager fileManager
      = new ExtendedStandardJavaFileManager(javac.getStandardFileManager(diagnosticCollector, null, null), compiledCode,
                                            dynamicClassLoader);
    JavaCompiler.CompilationTask task = javac.getTask(null, fileManager, diagnosticCollector, null, null,
                                                      compilationUnits);
    boolean success = task.call();

    if (!success) {
      List<Diagnostic<? extends JavaFileObject>> diagnostics = diagnosticCollector.getDiagnostics();
      for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics) {
        // read error dertails from the diagnostic object
        System.out.println(diagnostic.getMessage(null));
        System.out.println(diagnostic.getCode());
        System.out.println(diagnostic.getLineNumber());
      }
    }

    return dynamicClassLoader.loadClass(className);
  }

  private static class SourceCode extends SimpleJavaFileObject {

    private final String contents;

    SourceCode(String className, String contents) {
      super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
      this.contents = contents;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
      return contents;
    }
  }

  private static class CompiledCode extends SimpleJavaFileObject {
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();

    CompiledCode(String className) throws URISyntaxException {
      super(new URI(className), Kind.CLASS);
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
      return baos;
    }

    public byte[] getByteCode() {
      return baos.toByteArray();
    }
  }

  private static class ExtendedStandardJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

    private final CompiledCode compiledCode;
    private final ClassLoader classLoader;

    protected ExtendedStandardJavaFileManager(JavaFileManager fileManager, CompiledCode compiledCode,
                                              ClassLoader classLoader) {
      super(fileManager);
      this.compiledCode = compiledCode;
      this.classLoader = classLoader;
    }

    @Override
    public JavaFileObject getJavaFileForOutput(JavaFileManager.Location location, String className,
                                               JavaFileObject.Kind kind, FileObject sibling) throws IOException {
      return compiledCode;
    }

    @Override
    public ClassLoader getClassLoader(JavaFileManager.Location location) {
      return classLoader;
    }
  }

  public class DynamicClassLoader extends ClassLoader {

    private Map<String, CompiledCode> customCompiledCode = new HashMap<>();

    public DynamicClassLoader(ClassLoader parent) {
      super(parent);
    }

    public void setCode(CompiledCode cc) {
      customCompiledCode.put(cc.getName(), cc);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      CompiledCode cc = customCompiledCode.get(name);
      if (cc == null) {
        return super.findClass(name);
      }
      byte[] byteCode = cc.getByteCode();
      return defineClass(name, byteCode, 0, byteCode.length);
    }
  }
}

package co.cask.hydrator.plugin.transform;

/**
 * Created by alianwar on 12/2/16.
 */
public class CustomTransform2 {
  public static final String FOO = //"package co.cask.hydrator.plugin.transform;\n" +
    "\n" +
    "import co.cask.cdap.api.data.format.StructuredRecord;\n" +
    "import co.cask.cdap.etl.api.Emitter;\n" +
    "import co.cask.cdap.etl.api.Transform;\n" +
    "\n" +
    "public class CustomTransform extends Transform<StructuredRecord, StructuredRecord> {\n" +
    "\n" +
    "  @Override\n" +
    "  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {\n" +
    "    emitter.emit(structuredRecord);\n" +
    "  }\n" +
    "}\n";
}

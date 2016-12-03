package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;

public class CustomTransform extends Transform<StructuredRecord, StructuredRecord> {

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(structuredRecord);
  }
}

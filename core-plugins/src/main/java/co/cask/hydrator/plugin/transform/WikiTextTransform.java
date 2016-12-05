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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.engine.EngineException;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfig;
import org.sweble.wikitext.engine.nodes.EngProcessedPage;
import org.sweble.wikitext.engine.utils.DefaultConfigEnWp;
import org.sweble.wikitext.example.TextConverter;
import org.sweble.wikitext.parser.parser.LinkTargetException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = "transform")
@Name("WikiTextTransform")
@Description("convert wikitext to plain text")
public class WikiTextTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(WikiTextTransform.class);
  private static final Gson GSON = new Gson();
  private final WikiTextTransformConfig config;

  public WikiTextTransform(WikiTextTransformConfig config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("In transform");
    String wikiText = input.get(config.contentsField);
    WikiTitleAndText titleAndText = parse(wikiText.getBytes());
    String plainText = toPlainText(titleAndText);

    // Generate output Schema
    List<Schema.Field> outputFields = new ArrayList<>();
    outputFields.add(Schema.Field.of("plainText", Schema.of(Schema.Type.STRING)));

    for (Schema.Field field : input.getSchema().getFields()) {
      if (!field.getName().equals(config.contentsField)) {
        outputFields.add(Schema.Field.of(field.getName(), field.getSchema()));
      }
    }

    Schema outputSchema = Schema.recordOf("plainText", outputFields);

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema)
      .set("plainText", plainText);

    LOG.info("plainText: {}", plainText);

    for (Schema.Field field : input.getSchema().getFields()) {
      if (!field.getName().equals(config.contentsField)) {
        builder.set(field.getName(), input.get(field.getName()));
      }
    }
    emitter.emit(builder.build());

  }

  private WikiTitleAndText parse(byte[] rawWikiData) {
    WikiContents json = GSON.fromJson(Bytes.toString(rawWikiData), WikiContents.class);
    Map<String, WikiContents.Query.Page> pages = json.query.pages;
    // there is only one entry in this map, with the key as the page id
    WikiContents.Query.Page page = pages.get(pages.keySet().iterator().next());
    List<WikiContents.Query.Page.Content> revisions = page.revisions;
    // we always get the latest revision
    if (revisions.isEmpty()) {
      return null;
    }
    WikiContents.Query.Page.Content content = revisions.get(revisions.size() - 1);
    return new WikiTitleAndText(page.title, content.contents);
  }

  /**
   * Converts text formatted as text/wiki into text/plain using Sweble - http://sweble.org/
   */
  private String toPlainText(WikiTitleAndText titleAndText) throws EngineException, LinkTargetException {
    // Generate a Sweble WikiConfig
    WikiConfig config = DefaultConfigEnWp.generate();
    WtEngineImpl wtEngine = new WtEngineImpl(config);
    PageTitle pageTitle = PageTitle.make(config, titleAndText.title);
    PageId pageId = new PageId(pageTitle, 0);
    // Process the text/wiki using WtEngine
    EngProcessedPage processedPage = wtEngine.postprocess(pageId, titleAndText.contents, null);
    // Use a TextConverter to convert the processed page into Text.
    // WtEngine also allows to convert to HTML, but we want to do plain text analysis.
    TextConverter plainTextConverter = new TextConverter(config, 120);
    return (String) plainTextConverter.go(processedPage.getPage());
  }

  /**
   * Class to represent response body of wikidata API at https://www.mediawiki.org/wiki/API:Query
   */
  @SuppressWarnings("unused")
  private static final class WikiContents {
    private String batchcomplete;
    private Query query;

    private static final class Query {
      private List<Normalized> normalized;

      private static final class Normalized {
        private String from;
        private String to;
      }

      private Map<String, Page> pages;

      private static final class Page {
        private long pageid;
        private long ns;
        private String title;
        private List<Content> revisions;

        private static final class Content {
          private String contentformat;
          private String contentmodel;
          @SerializedName("*")
          private String contents;
        }
      }
    }
  }

  private static final class WikiTitleAndText {
    private final String title;
    private final String contents;

    private WikiTitleAndText(String title, String contents) {
      this.title = title;
      this.contents = contents;
    }
  }

  /**
   * Config class for ProjectionTransform
   */
  public static class WikiTextTransformConfig extends PluginConfig {

    @Name("contentsField")
    @Description("Field for converting wiki text to plain text")
    String contentsField;

    public WikiTextTransformConfig(String contentsField) {
      this.contentsField = contentsField;
    }
  }
}

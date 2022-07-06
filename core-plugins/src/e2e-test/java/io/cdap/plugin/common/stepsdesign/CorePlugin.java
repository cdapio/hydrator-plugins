package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.joiner.stepsdesign.Joiner;
import io.cucumber.java.en.Then;
import org.apache.directory.api.util.Strings;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.cdap.plugin.common.stepsdesign.TestSetupHooks.gcsTargetBucketName;

/**
 *  Core Plugin Related Step Design.
 */
public class CorePlugin implements CdfHelper {
  @Then("Verify the CSV Output File matches the Expected Output File: {string} With Expected Partition: {string}")
  public void verifyCSVOutput(String file, String partition) {
    try {
      // The output gcs folder will be like:
      // e2e-test-[uuid]
      // --2022-06-26-00-27/
      // ----_SUCCESS
      // ----part-r-0000
      // ----part-r-0001
      // ----part-r-...
      // The number of part-r-* files should match the expected partitions.
      int partitions = 0;
      List<String> lst = new ArrayList<>();
      for (Blob blob : StorageClient.listObjects(gcsTargetBucketName).iterateAll()) {
        String name = blob.getName();
        if (name.contains("part-r")) {
          partitions++;
          try (InputStream inputStream = new ByteArrayInputStream(blob.getContent())) {
            readInputStream(inputStream, lst);
          }
        }
      }
      Path path = Paths.get(Objects.requireNonNull(Joiner.class.getResource
        ("/" + PluginPropertyUtils.pluginProp(file))).getPath()).toAbsolutePath();
      Assert.assertEquals("Output partition should match",
                          partitions, Integer.parseInt(PluginPropertyUtils.pluginProp(partition)));
      String s1 = getSortedCSVContent(lst), s2 = new String(Files.readAllBytes(path));
      Assert.assertTrue("Output content should match",
                        Strings.equals(getSortedCSVContent(lst), new String(Files.readAllBytes(path))));
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + gcsTargetBucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
  private String getSortedCSVContent(List<String> lst) {
    // Since the spark output files aren't guaranteed to be ordered, the output entry with [id, first_name, last_name]
    // schema needs to be sorted by id for comparison purpose.
    lst.sort((s1, s2) -> {
      String id1 = s1.split(",")[0], id2 = s2.split(",")[0];
      return Integer.parseInt(id1) - Integer.parseInt(id2);
    });
    StringBuilder sb = new StringBuilder();
    for (String s : lst) {
      sb.append(s);
    }
    return sb.toString();
  }

  private void readInputStream(InputStream input, List<String> lst) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lst.add(line + "\n");
      }
    }
  }
}

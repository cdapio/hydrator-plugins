/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.batch.action;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link WindowsShareCopy.WindowsShareCopyConfig}
 */
public class WindowsShareCopyConfigTest {

    private WindowsShareCopy.WindowsShareCopyConfig createConfig(String sourceDirectory, String smbVersion) {
        return new WindowsShareCopy(null).new WindowsShareCopyConfig(
                "testDomain",
                "testHostname",
                "testUser",
                "testPassword",
                "testShare",
                sourceDirectory,
                "/testDest",
                4096,
                1,
                "true",
                smbVersion
        );
    }

    @Test
    public void testPathNormalization() {
        // 1. Test multiple leading forward slashes
        WindowsShareCopy.WindowsShareCopyConfig config1 = createConfig("///folder/subfolder", "SMBv2/v3");
        Assert.assertEquals("folder\\subfolder", config1.getSourceDirectory());

        // 2. Test multiple leading backward slashes
        WindowsShareCopy.WindowsShareCopyConfig config2 = createConfig("\\\\\\folder\\subfolder", "SMBv2/v3");
        Assert.assertEquals("folder\\subfolder", config2.getSourceDirectory());

        // 3. Test mixed/no leading slashes but internal forward slashes
        WindowsShareCopy.WindowsShareCopyConfig config3 = createConfig("folder/subfolder/file.txt", "SMBv2/v3");
        Assert.assertEquals("folder\\subfolder\\file.txt", config3.getSourceDirectory());

        // 4. Test empty string
        WindowsShareCopy.WindowsShareCopyConfig config4 = createConfig("", "SMBv2/v3");
        Assert.assertEquals("", config4.getSourceDirectory());

        // 5. Test null
        WindowsShareCopy.WindowsShareCopyConfig config5 = createConfig(null, "SMBv2/v3");
        Assert.assertNull(config5.getSourceDirectory());
    }

    @Test
    public void testSMBVersionFallback() {
        // 1. Test null falls back to true (SMBv1) for backward compatibility
        WindowsShareCopy.WindowsShareCopyConfig nullConfig = createConfig("/dir", null);
        Assert.assertTrue(nullConfig.isSMBv1());

        // 2. Test explicit SMBv1
        WindowsShareCopy.WindowsShareCopyConfig v1Config = createConfig("/dir", "SMBv1");
        Assert.assertTrue(v1Config.isSMBv1());

        // 2b. Test explicit SMBv1 (case insensitive)
        WindowsShareCopy.WindowsShareCopyConfig v1ConfigLower = createConfig("/dir", "smbv1");
        Assert.assertTrue(v1ConfigLower.isSMBv1());

        // 3. Test explicit SMBv2/v3
        WindowsShareCopy.WindowsShareCopyConfig v2Config = createConfig("/dir", "SMBv2/v3");
        Assert.assertFalse(v2Config.isSMBv1());
    }

    @Test
    public void testValidConfiguration() {
        WindowsShareCopy.WindowsShareCopyConfig config = createConfig("/dir", "SMBv2/v3");
        MockFailureCollector collector = new MockFailureCollector();
        config.validate(collector);

        Assert.assertEquals(0, collector.getValidationFailures().size());
    }

    @Test
    public void testInvalidConfiguration() {
        // Create a config with null for all required fields
        WindowsShareCopy.WindowsShareCopyConfig invalidConfig =
                new WindowsShareCopy(null).new WindowsShareCopyConfig(
                null, null, null, null,
                        null, null, null, 4096,
                        1, "true", "SMBv2/v3"
        );

        MockFailureCollector collector = new MockFailureCollector();
        invalidConfig.validate(collector);

        // Should trigger 5 validation failures (hostname, username, password, sharename, sourcedir, destdir)
        Assert.assertEquals(6, collector.getValidationFailures().size());
    }
}

/*
 * Copyright (C) 2026 Tencent. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.goosefs;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/** OS classifier and binding version. */
public enum Environment {
    INSTANCE;

    public static final String UNKNOWN = "<unknown>";
    private String classifier = UNKNOWN;
    private String projectVersion = UNKNOWN;

    static {
        ClassLoader classLoader = Environment.class.getClassLoader();
        try (InputStream is = classLoader.getResourceAsStream("bindings.properties")) {
            final Properties properties = new Properties();
            if (is != null) {
                properties.load(is);
                INSTANCE.projectVersion = properties.getProperty("project.version", UNKNOWN);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot load environment properties file", e);
        }

        final StringBuilder classifier = new StringBuilder();
        final String os = System.getProperty("os.name").toLowerCase();
        if (os.startsWith("windows")) {
            classifier.append("windows");
        } else if (os.startsWith("mac")) {
            classifier.append("osx");
        } else {
            classifier.append("linux");
        }
        classifier.append("-");
        final String arch = System.getProperty("os.arch").toLowerCase();
        if (arch.equals("aarch64")) {
            classifier.append("aarch_64");
        } else {
            classifier.append("x86_64");
        }
        if (classifier.indexOf("linux-") == 0 && isMuslRuntime()) {
            classifier.append("-musl");
        }
        INSTANCE.classifier = classifier.toString();
    }

    public static String getClassifier() {
        return INSTANCE.classifier;
    }

    public static String getVersion() {
        return INSTANCE.projectVersion;
    }

    private static boolean isMuslRuntime() {
        return hasMuslLoader(Paths.get("/lib")) || hasMuslLoader(Paths.get("/usr/lib"));
    }

    private static boolean hasMuslLoader(Path dir) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "ld-musl-*.so.1")) {
            return stream.iterator().hasNext();
        } catch (IOException | SecurityException e) {
            return false;
        }
    }
}

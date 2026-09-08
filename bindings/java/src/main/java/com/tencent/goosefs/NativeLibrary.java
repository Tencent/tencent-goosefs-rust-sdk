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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicReference;

/** Loads {@code goosefs_java} from {@code java.library.path} or a classified JAR. */
public final class NativeLibrary {
    private enum LibraryState {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    private static final AtomicReference<LibraryState> LIBRARY_LOADED = new AtomicReference<>(LibraryState.NOT_LOADED);

    private NativeLibrary() {}

    public static void loadLibrary() {
        if (LIBRARY_LOADED.get() == LibraryState.LOADED) {
            return;
        }
        if (LIBRARY_LOADED.compareAndSet(LibraryState.NOT_LOADED, LibraryState.LOADING)) {
            try {
                doLoadLibrary();
            } catch (IOException e) {
                LIBRARY_LOADED.set(LibraryState.NOT_LOADED);
                throw new UncheckedIOException("Unable to load the GooseFS shared library", e);
            } catch (UnsatisfiedLinkError e) {
                LIBRARY_LOADED.set(LibraryState.NOT_LOADED);
                throw e;
            }
            LIBRARY_LOADED.set(LibraryState.LOADED);
            return;
        }
        while (LIBRARY_LOADED.get() == LibraryState.LOADING) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void doLoadLibrary() throws IOException {
        try {
            System.loadLibrary("goosefs_java");
            return;
        } catch (UnsatisfiedLinkError ignore) {
            // try bundled
        }
        doLoadBundledLibrary();
    }

    private static void doLoadBundledLibrary() throws IOException {
        final String libraryPath = bundledLibraryPath();
        UnsatisfiedLinkError linkError = null;
        try (InputStream is = NativeObject.class.getResourceAsStream(libraryPath)) {
            if (is != null) {
                loadFromStream(libraryPath, is);
                return;
            }
        }

        final String fallbackLibraryPath = fallbackBundledLibraryPath();
        if (fallbackLibraryPath == null) {
            if (linkError != null) {
                throw linkError;
            }
            throw new IOException("cannot find " + libraryPath);
        }
        try (InputStream is = NativeObject.class.getResourceAsStream(fallbackLibraryPath)) {
            if (is == null) {
                if (linkError != null) {
                    throw linkError;
                }
                throw new IOException("cannot find " + libraryPath);
            }
            try {
                loadFromStream(fallbackLibraryPath, is);
            } catch (UnsatisfiedLinkError e) {
                throw e;
            }
        }
    }

    private static void loadFromStream(String libraryPath, InputStream is) throws IOException {
        final int dot = libraryPath.indexOf('.', libraryPath.lastIndexOf('/') + 1);
        final File tmpFile =
                File.createTempFile(libraryPath.substring(1, dot).replace('/', '_'), libraryPath.substring(dot));
        tmpFile.deleteOnExit();
        Files.copy(is, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.load(tmpFile.getAbsolutePath());
    }

    private static String bundledLibraryPath() {
        return "/native/" + Environment.getClassifier() + "/" + System.mapLibraryName("goosefs_java");
    }

    private static String fallbackBundledLibraryPath() {
        final String classifier = Environment.getClassifier();
        if (!classifier.startsWith("linux-")) {
            return null;
        }
        final String libraryName = System.mapLibraryName("goosefs_java");
        if (classifier.endsWith("-musl")) {
            final String gnu = classifier.substring(0, classifier.length() - "-musl".length());
            return "/native/" + gnu + "/" + libraryName;
        }
        return "/native/" + classifier + "-musl/" + libraryName;
    }
}

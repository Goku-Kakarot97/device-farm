package com.mservice.farmstream.adb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AdbServerTest {

    private static Path executable(Path dir, String... parts) throws Exception {
        Path p = dir;
        for (String part : parts) {
            p = p.resolve(part);
        }
        Files.createDirectories(p.getParent());
        Files.writeString(p, "#!/bin/sh\n");
        Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rwxr-xr-x"));
        return p;
    }

    @Test
    void explicitBinWins(@TempDir Path tmp) throws Exception {
        Path bin = executable(tmp, "custom", "adb");
        Path onPath = executable(tmp, "bin", "adb");
        assertEquals(bin, AdbServer.resolveBinary(bin.toString(), onPath.getParent().toString(), null, null, null));
    }

    @Test
    void pathThenSdkThenHome(@TempDir Path tmp) throws Exception {
        Path onPath = executable(tmp, "bin", "adb");
        Path sdk = executable(tmp, "sdk", "platform-tools", "adb");
        Path home = executable(tmp, "home", "Library", "Android", "sdk", "platform-tools", "adb");
        String path = tmp.resolve("missing") + File.pathSeparator + onPath.getParent();
        assertEquals(onPath, AdbServer.resolveBinary(null, path, tmp.resolve("sdk").toString(), null, tmp.resolve("home").toString()));
        assertEquals(sdk, AdbServer.resolveBinary(null, tmp.resolve("missing").toString(), tmp.resolve("sdk").toString(), null, tmp.resolve("home").toString()));
        assertEquals(home, AdbServer.resolveBinary(null, null, null, null, tmp.resolve("home").toString()));
    }

    @Test
    void nothingFound(@TempDir Path tmp) throws Exception {
        Files.writeString(tmp.resolve("adb"), "not executable");
        assertNull(AdbServer.resolveBinary(null, tmp.toString(), tmp.toString(), null, tmp.toString()));
    }
}

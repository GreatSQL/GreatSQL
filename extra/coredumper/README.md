The coredumper library can be compiled into applications to create
core dumps of the running program, without having to terminate
them. It supports both single- and multi-threaded core dumps, even if
the kernel does not have native support for multi-threaded core files.

This library is primarily intended to simplify debugging of
long-running services. It is often inacceptable to suspend production
services by attaching a debugger, nor is it possible to crash the
service in order to generate a core file.

By modifying an existing service to take advantage of the coredumper
library, it is possible to expose an interface for obtaining snapshots
of the running application. The library supports writing of core files
to disk (e.g. triggered upon reception of a signal) but it can also
generate in-memory core files.  This makes it possible for web
services to expose remote access to core files.

The "examples" directory shows how to add a core file feature to an
existing TFTP server. For an example of how to use on-disk core files,
take a look at "src/coredump_unittest.c".

The code has been tested on Linux x86/64. It is
available as a git source archive, and can be build using CMake.

To configure the library, execute CMake first by running:

```
cmake <src_dir> -DCMAKE_BUILD_TYPE=(Debug|RelWithDebInfo|Release) \
                -DBUILD_SHARED_LIBS=(ON|OFF) \
                -DCMAKE_INSTALL_PREFIX=<install_dir> \
                -DBUILD_TESTING=(ON|OFF)
                -G<build_system>
```

All options are optional, and the boolean options default to `ON`.
The library can be used both in the build tree and at the install location,
both using `add_subdirectory` and using the exported cmake configurations.

After configuring, run Make/Ninja/anythnig:

```
ninja
```

If the tests were built, they can be executed using CTest:

```
ctest .
```

The library can be installed using the install target:

```
ninja install
```
The preferred method of using the library is by using the provided
cmake configuration files. These configuration files can be found at:

 * CMAKE_BUILD_DIR/src/libcoredumper.cmake
 * CMAKE_INSTALL_DIR/cmake/libcoredumper.cmake

For more information on how to use the library, read the manual pages
for "GetCoreDump" and "WriteCoreDump".

14 May 2020

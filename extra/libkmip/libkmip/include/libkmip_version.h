#ifndef LIBKMIP_VERSION_H
#define LIBKMIP_VERSION_H


#define KMIP_LIB_VERSION_MAJOR 0
#define KMIP_LIB_VERSION_MINOR 3
#define KMIP_LIB_VERSION_PATCH 2

#define KMIP_LIB_STRINGIFY_I(x) #x
#define KMIP_LIB_TOSTRING_I(x)  KMIP_LIB_STRINGIFY_I (x)

#define KMIP_LIB_VERSION_STR                                                                                           \
  KMIP_LIB_TOSTRING_I (KMIP_LIB_VERSION_MAJOR)                                                                         \
  "." KMIP_LIB_TOSTRING_I (KMIP_LIB_VERSION_MINOR) "." KMIP_LIB_TOSTRING_I (KMIP_LIB_VERSION_PATCH)


#endif // LIBKMIP_VERSION_H

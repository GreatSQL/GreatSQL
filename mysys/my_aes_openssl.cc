/* Copyright (c) 2015, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.

This program is also distributed with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have included with MySQL.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License, version 2.0, for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file mysys/my_aes_openssl.cc
*/

#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include <openssl/aes.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "m_string.h"
#include "my_aes.h"
#include "my_aes_impl.h"
#include "mysys/my_kdf.h"

/*
  xplugin needs BIO_new_bio_pair, but the server does not.
  Add an explicit dependency here, so that it is available when loading
  the plugin.
 */
int dummy_function_needed_by_xplugin() {
  BIO *bio1;
  BIO *bio2;
  return BIO_new_bio_pair(&bio1, 42U, &bio2, 42U);
}

/* keep in sync with enum my_aes_opmode in my_aes.h */
const char *my_aes_opmode_names[] = {
    "aes-128-ecb",    "aes-192-ecb",    "aes-256-ecb",    "aes-128-cbc",
    "aes-192-cbc",    "aes-256-cbc",    "aes-128-cfb1",   "aes-192-cfb1",
    "aes-256-cfb1",   "aes-128-cfb8",   "aes-192-cfb8",   "aes-256-cfb8",
    "aes-128-cfb128", "aes-192-cfb128", "aes-256-cfb128", "aes-128-ofb",
    "aes-192-ofb",    "aes-256-ofb",    "des_ede3_cbc",
#ifdef SSL_GM
    "sm4_ecb",        "sm4_cbc",        "sm4_cfb",        "sm4_ofb",
#endif
    nullptr /* needed for the type
                                         enumeration */
};

/* keep in sync with enum my_aes_opmode in my_aes.h */
static uint my_aes_opmode_key_sizes_impl[] = {
    128 /* aes-128-ecb */,
    192 /* aes-192-ecb */,
    256 /* aes-256-ecb */,
    128 /* aes-128-cbc */,
    192 /* aes-192-cbc */,
    256 /* aes-256-cbc */,
    128 /* aes-128-cfb1 */,
    192 /* aes-192-cfb1 */,
    256 /* aes-256-cfb1 */,
    128 /* aes-128-cfb8 */,
    192 /* aes-192-cfb8 */,
    256 /* aes-256-cfb8 */,
    128 /* aes-128-cfb128 */,
    192 /* aes-192-cfb128 */,
    256 /* aes-256-cfb128 */,
    128 /* aes-128-ofb */,
#ifdef SSL_GM
    192 /* aes-192-ofb */,
    256 /* aes-256-ofb */,
    192 /* des_ede3_cbc */,
    128 /* sm4-ecb*/,
    128 /* sm4-cbc*/,
    128 /* sm4-cfb*/,
    128 /*sm4-ofb*/
#else
    192 /* aes-192-ofb */,
    256 /* aes-256-ofb */,
    192 /* des_ede3_cbc */
#endif
};

uint *my_aes_opmode_key_sizes = my_aes_opmode_key_sizes_impl;

static const EVP_CIPHER *aes_evp_type(const my_aes_opmode mode) {
  switch (mode) {
    case my_aes_128_ecb:
      return EVP_aes_128_ecb();
    case my_aes_128_cbc:
      return EVP_aes_128_cbc();
    case my_aes_128_cfb1:
      return EVP_aes_128_cfb1();
    case my_aes_128_cfb8:
      return EVP_aes_128_cfb8();
    case my_aes_128_cfb128:
      return EVP_aes_128_cfb128();
    case my_aes_128_ofb:
      return EVP_aes_128_ofb();
    case my_aes_192_ecb:
      return EVP_aes_192_ecb();
    case my_aes_192_cbc:
      return EVP_aes_192_cbc();
    case my_aes_192_cfb1:
      return EVP_aes_192_cfb1();
    case my_aes_192_cfb8:
      return EVP_aes_192_cfb8();
    case my_aes_192_cfb128:
      return EVP_aes_192_cfb128();
    case my_aes_192_ofb:
      return EVP_aes_192_ofb();
    case my_aes_256_ecb:
      return EVP_aes_256_ecb();
    case my_aes_256_cbc:
      return EVP_aes_256_cbc();
    case my_aes_256_cfb1:
      return EVP_aes_256_cfb1();
    case my_aes_256_cfb8:
      return EVP_aes_256_cfb8();
    case my_aes_256_cfb128:
      return EVP_aes_256_cfb128();
    case my_aes_256_ofb:
      return EVP_aes_256_ofb();
    case my_des_ede3_cbc:
      return EVP_des_ede3_cbc();
#ifdef SSL_GM
    case my_sm4_ecb:
      return EVP_sm4_ecb();
    case my_sm4_cbc:
      return EVP_sm4_cbc();
    case my_sm4_cfb:
      return EVP_sm4_cfb128();
    case my_sm4_ofb:
      return EVP_sm4_ofb();
#endif
    default:
      return nullptr;
  }
}

/**
  Creates required length of AES key,
  Input key size can be smaller or bigger in length, we need exact AES key
  size. If KDF options are valid and given, use KDF functionality. otherwise
  use previously used method.

  @param [out] rkey Output key
  @param key Input key
  @param key_length input key length
  @param mode AES mode
  @param kdf_options  KDF function options

  @return 0 on success and 1 on failure
*/
int my_create_key(unsigned char *rkey, const unsigned char *key,
                  uint32 key_length, enum my_aes_opmode mode,
                  std::vector<std::string> *kdf_options) {
  if (kdf_options) {
    if (kdf_options->size() < 1) {
      return 1;
    }
    const uint key_size = my_aes_opmode_key_sizes[mode] / 8;
    return create_kdf_key(key, key_length, rkey, key_size, kdf_options);
  }

  my_aes_create_key(key, key_length, rkey, mode);
  return 0;
}

int my_aes_encrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest, const unsigned char *key,
                   uint32 key_length, enum my_aes_opmode mode,
                   const unsigned char *iv, bool padding,
                   std::vector<std::string> *kdf_options) {
  if (my_des_is(mode))
    return my_des_encrypt_decrypt(source, source_length, dest, key, key_length,
                                  mode, iv, 1, false, kdf_options);

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX stack_ctx;
  EVP_CIPHER_CTX *ctx = &stack_ctx;
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  const EVP_CIPHER *cipher = aes_evp_type(mode);
  int u_len, f_len;
  /* The real key to be used for encryption */
  unsigned char rkey[MAX_AES_KEY_LENGTH / 8];

  if (my_create_key(rkey, key, key_length, mode, kdf_options)) {
    return MY_AES_BAD_DATA;
  }
  if (!ctx || !cipher || (EVP_CIPHER_iv_length(cipher) > 0 && !iv))
    return MY_AES_BAD_DATA;

  if (!EVP_EncryptInit(ctx, cipher, rkey, iv)) goto aes_error;   /* Error */
  if (!EVP_CIPHER_CTX_set_padding(ctx, padding)) goto aes_error; /* Error */
  if (!EVP_EncryptUpdate(ctx, dest, &u_len, source, source_length))
    goto aes_error; /* Error */

  if (!EVP_EncryptFinal(ctx, dest + u_len, &f_len)) goto aes_error; /* Error */

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX_cleanup(ctx);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX_free(ctx);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  return u_len + f_len;

aes_error:
  /* need to explicitly clean up the error if we want to ignore it */
  ERR_clear_error();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX_cleanup(ctx);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX_free(ctx);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  return MY_AES_BAD_DATA;
}

int my_aes_decrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest, const unsigned char *key,
                   uint32 key_length, enum my_aes_opmode mode,
                   const unsigned char *iv, bool padding,
                   std::vector<std::string> *kdf_options) {
  if (my_des_is(mode))
    return my_des_encrypt_decrypt(source, source_length, dest, key, key_length,
                                  mode, iv, 0, false, kdf_options);

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX stack_ctx;
  EVP_CIPHER_CTX *ctx = &stack_ctx;
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  const EVP_CIPHER *cipher = aes_evp_type(mode);
  int u_len, f_len;

  /* The real key to be used for decryption */
  unsigned char rkey[MAX_AES_KEY_LENGTH / 8];

  if (my_create_key(rkey, key, key_length, mode, kdf_options)) {
    return MY_AES_BAD_DATA;
  }

  if (!ctx || !cipher || (EVP_CIPHER_iv_length(cipher) > 0 && !iv))
    return MY_AES_BAD_DATA;

  if (!EVP_DecryptInit(ctx, aes_evp_type(mode), rkey, iv))
    goto aes_error;                                              /* Error */
  if (!EVP_CIPHER_CTX_set_padding(ctx, padding)) goto aes_error; /* Error */
  if (!EVP_DecryptUpdate(ctx, dest, &u_len, source, source_length))
    goto aes_error; /* Error */
  if (!EVP_DecryptFinal_ex(ctx, dest + u_len, &f_len))
    goto aes_error; /* Error */

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX_cleanup(ctx);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX_free(ctx);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */

  return u_len + f_len;

aes_error:
  /* need to explicitly clean up the error if we want to ignore it */
  ERR_clear_error();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX_cleanup(ctx);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX_free(ctx);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  return MY_AES_BAD_DATA;
}

int my_des_encrypt_decrypt(const unsigned char *source, uint32 source_length,
                           unsigned char *dest, const unsigned char *key,
                           uint32 key_length, enum my_aes_opmode mode,
                           const unsigned char *iv, int encrypt_decrypt,
                           bool padding,
                           std::vector<std::string> *kdf_options) {
  const unsigned char ivd_default[8] = {0};
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!(ctx)) return MY_AES_BAD_DATA;
  const EVP_CIPHER *cipher = aes_evp_type(mode);
  int u_len, f_len;
  /* The real key to be used for encryption */
  unsigned char rkey[MAX_DES_KEY_LENGTH / 8];

  if (my_create_key(rkey, key, key_length, mode, kdf_options)) {
    EVP_CIPHER_CTX_free(ctx);
    return MY_AES_BAD_DATA;
  }

  if ((!cipher) && (EVP_CIPHER_iv_length(cipher) > 0 && !iv)) {
    EVP_CIPHER_CTX_free(ctx);
    return MY_AES_BAD_DATA;
  }

  if (!iv || !strlen(reinterpret_cast<const char *>(iv))) iv = ivd_default;
  if (EVP_CipherInit_ex(ctx, cipher, NULL, rkey, iv, encrypt_decrypt) != 1)
    goto aes_error;                                              /* Error */
  if (!EVP_CIPHER_CTX_set_padding(ctx, padding)) goto aes_error; /* Error */
  if (EVP_CipherUpdate(ctx, dest, &u_len, (const unsigned char *)source,
                       source_length) != 1)
    goto aes_error; /* Error */

  if (EVP_CipherFinal_ex(ctx, dest + u_len, &f_len) != 1)
    goto aes_error; /* Error */

  EVP_CIPHER_CTX_free(ctx);
  return u_len;

aes_error:
  /* need to explicitly clean up the error if we want to ignore it */
  ERR_clear_error();
  EVP_CIPHER_CTX_free(ctx);
  return MY_AES_BAD_DATA;
}

#ifdef SSL_GM
int sm2_encrypt(const unsigned char *source, uint32 source_length,
                unsigned char *dest, size_t &dest_len, EVP_PKEY *sm2_key) {
  EVP_PKEY_CTX *pctx = EVP_PKEY_CTX_new(sm2_key, NULL);
  if (pctx == NULL) return 1;
  if (EVP_PKEY_encrypt_init(pctx) <= 0) goto error;
  if (EVP_PKEY_encrypt(pctx, NULL, &dest_len, source, source_length) <= 0)
    goto error;
  if (EVP_PKEY_encrypt(pctx, dest, &dest_len, source, source_length) <= 0)
    goto error;
  EVP_PKEY_CTX_free(pctx);
  EVP_PKEY_free(sm2_key);
  return 0;
error:
  ERR_clear_error();
  EVP_PKEY_CTX_free(pctx);
  EVP_PKEY_free(sm2_key);
  return 1;
}

int sm2_decrypt(const unsigned char *source, uint32 source_length,
                unsigned char *dest, size_t &dest_len, EVP_PKEY *sm2_key) {
  EVP_PKEY_CTX *pctx = EVP_PKEY_CTX_new(sm2_key, NULL);
  if (pctx == NULL) return 1;
  if (EVP_PKEY_decrypt_init(pctx) <= 0) goto error;
  if (EVP_PKEY_decrypt(pctx, NULL, &dest_len, source, source_length) <= 0)
    goto error;
  if (EVP_PKEY_decrypt(pctx, dest, &dest_len, source, source_length) <= 0)
    goto error;
  EVP_PKEY_CTX_free(pctx);
  EVP_PKEY_free(sm2_key);
  return 0;
error:
  ERR_clear_error();
  EVP_PKEY_CTX_free(pctx);
  EVP_PKEY_free(sm2_key);
  return 1;
}

int sm3(const unsigned char *msg, uint32 msg_length, unsigned char *dest) {
  EVP_MD_CTX *mdctx = NULL;
  mdctx = EVP_MD_CTX_new();
  if (mdctx == NULL) return 1;
  if (!EVP_DigestInit_ex(mdctx, EVP_sm3(), NULL)) return 1;
  if (!EVP_DigestUpdate(mdctx, msg, msg_length)) return 1;
  if (!EVP_DigestFinal_ex(mdctx, dest, nullptr)) return 1;
  EVP_MD_CTX_free(mdctx);
  return 0;
}

int hmac_sm3(const unsigned char *source, uint32 source_length,
             unsigned char *dest, const unsigned char *key, uint32 key_length) {
  unsigned int digest_len;
  HMAC_CTX *ctx = HMAC_CTX_new();
  if (ctx == NULL) return 1;
  if (!HMAC_Init_ex(ctx, key, key_length, EVP_sm3(), NULL)) return 1;
  if (!HMAC_Update(ctx, source, source_length)) return 1;
  if (!HMAC_Final(ctx, dest, &digest_len)) return 1;
  HMAC_CTX_free(ctx);
  return 0;
}
#endif /* SSL_GM */

longlong my_aes_get_size(uint32 source_length, my_aes_opmode opmode) {
  const EVP_CIPHER *cipher = aes_evp_type(opmode);
  size_t block_size;

  block_size = EVP_CIPHER_block_size(cipher);

  if (block_size <= 1) return source_length;
  return block_size * (static_cast<ulonglong>(source_length) / block_size) +
         (my_des_is(opmode) ? 0 : block_size);
}

bool my_aes_needs_iv(my_aes_opmode opmode) {
  const EVP_CIPHER *cipher = aes_evp_type(opmode);
  int iv_length;
  iv_length = EVP_CIPHER_iv_length(cipher);
  assert(iv_length == 0 || iv_length == MY_AES_IV_SIZE ||
         iv_length == MY_DES_IV_SIZE);
  return iv_length != 0 ? true : false;
}

static int my_legacy_aes_cbc_nopad_crypt(
    bool encrypt, const unsigned char *source, uint32 source_length,
    unsigned char *dest, const unsigned char *key, uint32 key_length,
    const unsigned char *iv) {
  assert(key_length == 32 || key_length == 16);

  if (key == nullptr || iv == nullptr) return MY_AES_BAD_DATA;

  auto cipher =
      aes_evp_type(key_length == 32 ? my_aes_256_cbc : my_aes_128_cbc);
  assert(cipher != nullptr);

  auto evp_cipher_deleter = [](EVP_CIPHER_CTX *ctx) {
    if (ctx != nullptr)
#if OPENSSL_VERSION_NUMBER < 0x10100000L
      EVP_CIPHER_CTX_cleanup(ctx);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
      EVP_CIPHER_CTX_free(ctx);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  };
  using evp_cipher_ctx_ptr =
      std::unique_ptr<EVP_CIPHER_CTX, decltype(evp_cipher_deleter)>;

  auto clear_error_helper = [](int return_value) {
    if (return_value != MY_AES_BAD_DATA) ERR_clear_error();
    return return_value;
  };

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX stack_ctx;
  EVP_CIPHER_CTX *ctx_raw = &stack_ctx;
  EVP_CIPHER_CTX_init(ctx_raw);
#else  /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  EVP_CIPHER_CTX *ctx_raw = EVP_CIPHER_CTX_new();
  if (ctx_raw == nullptr) return clear_error_helper(MY_AES_BAD_DATA);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */

  evp_cipher_ctx_ptr ctx(ctx_raw, std::move(evp_cipher_deleter));

  int u_len = 0, f_len = 0;

  if (!EVP_CipherInit_ex(ctx_raw, cipher, nullptr, key, iv, encrypt ? 1 : 0))
    return clear_error_helper(MY_AES_BAD_DATA);
  if (!EVP_CIPHER_CTX_set_padding(ctx_raw, 0))
    return clear_error_helper(MY_AES_BAD_DATA);
  if (!EVP_CipherUpdate(ctx_raw, dest, &u_len, source, source_length))
    return clear_error_helper(MY_AES_BAD_DATA);

  assert(static_cast<int>(source_length) >= u_len);
  std::size_t remainder_len = source_length - u_len;
  if (remainder_len > 0) {
    const unsigned char *remainder_source = source + u_len;
    unsigned char *remainder_dest = dest + u_len;
    /*
      Not much we can do, block ciphers cannot encrypt data that aren't
      a multiple of the block length. At least not without padding.
      Let's do something CTR-like for the last partial block.
    */
    unsigned char mask[MY_AES_BLOCK_SIZE];

    int mask_result = my_aes_encrypt(
        iv, sizeof(mask), mask, key, key_length,
        key_length == 32 ? my_aes_256_ecb : my_aes_128_ecb, nullptr, false);
    if (mask_result != MY_AES_BLOCK_SIZE)
      return clear_error_helper(MY_AES_BAD_DATA);

    for (std::size_t i = 0; i < remainder_len; ++i)
      remainder_dest[i] = remainder_source[i] ^ mask[i];
    f_len = remainder_len;
  }

  return clear_error_helper(u_len + f_len);
}

int my_legacy_aes_cbc_nopad_encrypt(const unsigned char *source,
                                    uint32 source_length, unsigned char *dest,
                                    const unsigned char *key, uint32 key_length,
                                    const unsigned char *iv) {
  return my_legacy_aes_cbc_nopad_crypt(true, source, source_length, dest, key,
                                       key_length, iv);
}

int my_legacy_aes_cbc_nopad_decrypt(const unsigned char *source,
                                    uint32 source_length, unsigned char *dest,
                                    const unsigned char *key, uint32 key_length,
                                    const unsigned char *iv) {
  return my_legacy_aes_cbc_nopad_crypt(false, source, source_length, dest, key,
                                       key_length, iv);
}

bool my_des_is(my_aes_opmode opmode) {
  switch (opmode) {
    case my_des_ede3_cbc:
      return true;
    default:
      return false;
  }
  return false;
}

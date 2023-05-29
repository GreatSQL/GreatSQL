/* Copyright (c) 2023, GreatDB Software Co., Ltd.
  All rights reserved.

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

#include <fcntl.h>

#include "client/client_priv.h"
#include "map_helpers.h"
#include "my_default.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql_version.h"
#include "path.h"
#include "print_version.h"
#include "sql/encrypt.h"
#include "welcome_copyright_notice.h" /* ORACLE_WELCOME_COPYRIGHT_NOTICE */

/* Exit codes */

#define EX_USAGE 1
#define EX_MYSQLERR 2

static char *opt_clone_decrypt_key = nullptr, *opt_clone_decrypt_file = nullptr,
            *opt_clone_decrypt_dir = nullptr;
static bool opt_clone_decrypt = false, opt_remove_original = false;

my_aes_opmode clone_encrypt_mode = my_aes_128_ecb;
char *clone_encrypt_key = nullptr;
char *clone_encrypt_iv = nullptr;

FILE *md_result_file = nullptr;
FILE *stderror_file = nullptr;

static struct my_option my_long_options[] = {
    {"help", '?', "Display this help and exit.", nullptr, nullptr, nullptr,
     GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"clone-decrypt", OPT_CLONE_DECRYPT, "Decrypt mysql clone encrypt file.",
     &opt_clone_decrypt, &opt_clone_decrypt, nullptr, GET_BOOL, NO_ARG, 0, 0, 0,
     nullptr, 0, nullptr},
    {"clone-decrypt-key", OPT_CLONE_DECRYPT_KEY, "Decrypt  key file.",
     &opt_clone_decrypt_key, &opt_clone_decrypt_key, nullptr, GET_STR,
     REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"clone-decrypt-file", OPT_CLONE_DECRYPT_FILE,
     "Decrypt clone encrypt file.", &opt_clone_decrypt_file,
     &opt_clone_decrypt_file, nullptr, GET_STR, REQUIRED_ARG, 0, 0, 0, nullptr,
     0, nullptr},
    {"clone-decrypt-dir", OPT_CLONE_DECRYPT_DIR, "Decrypt clone encrypt dir.",
     &opt_clone_decrypt_dir, &opt_clone_decrypt_dir, nullptr, GET_STR,
     REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"remove-original", OPT_REMOVE_ORIGINAL,
     "Remove .xbcrypt files "
     "after decryption ",
     (uchar *)&opt_remove_original, (uchar *)&opt_remove_original, 0, GET_BOOL,
     NO_ARG, 0, 0, 0, 0, 0, 0},
    {"result-file", 'r',
     "Direct output to a given file. This option should be used in systems "
     "(e.g., DOS, Windows) that use carriage-return linefeed pairs (\\r\\n) "
     "to separate text lines. This option ensures that only a single newline "
     "is used.",
     nullptr, nullptr, nullptr, GET_STR, REQUIRED_ARG, 0, 0, 0, nullptr, 0,
     nullptr},
    {nullptr, 0, nullptr, nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0,
     0, nullptr, 0, nullptr}};

static void usage() {
  print_version();
  puts(ORACLE_WELCOME_COPYRIGHT_NOTICE("2000"));
  printf(
      "\
Decrypt the encrypt files which clone encrypt backup \n\
the mysql command line client.\n\n");
  printf("Usage: %s [options] \n", my_progname);
  my_print_help(my_long_options);
  my_print_variables(my_long_options);
}

static bool get_one_option(int optid,
                           const struct my_option *opt MY_ATTRIBUTE((unused)),
                           char *argument) {
  switch (optid) {
    case 'r':
      if (!(md_result_file =
                my_fopen(argument, O_WRONLY | MY_FOPEN_BINARY, MYF(MY_WME))))
        exit(1);
      break;
    case 'V':
      print_version();
      exit(0);
    case 'I':
    case '?':
      usage();
      exit(0);
      break;
  }
  return false;
}

static void free_resources() {
  my_free(clone_encrypt_key);
  my_free(clone_encrypt_iv);
}

static int get_options(int *argc, char ***argv) {
  int ho_error;

  md_result_file = stdout;

  if ((ho_error = handle_options(argc, argv, my_long_options, get_one_option)))
    return (ho_error);

  if (!opt_clone_decrypt && ((*argc < 1) || (*argc > 4))) {
    usage();
    return EX_USAGE;
  }
  return (0);
} /* get_options */

int main(int argc, char **argv) {
  int exit_code;
  MY_INIT("mysqldecrypt");

  exit_code = get_options(&argc, &argv);
  if (exit_code) {
    free_resources();
    exit(exit_code);
  }

  if (opt_clone_decrypt) {
    if (!opt_clone_decrypt_key) {
      fprintf(stderr, "clone-decrypt-keyshould be specified.\n");
      free_resources();
      exit(EX_MYSQLERR);
    }
    if (!(opt_clone_decrypt_file || opt_clone_decrypt_dir)) {
      fprintf(stderr,
              "clone-decrypt-file or clone-decrypt-dir should be specified.\n");
      free_resources();
      exit(EX_MYSQLERR);
    }

    if (opt_clone_decrypt_file && opt_clone_decrypt_dir) {
      fprintf(stderr,
              "only one of the two parameters clone-decrypt-file and "
              "clone-decrypt-dir can be set.\n");
      free_resources();
      exit(EX_MYSQLERR);
    }

    exit_code =
        read_clone_encrypt_key(opt_clone_decrypt_key, &clone_encrypt_mode,
                               &clone_encrypt_key, &clone_encrypt_iv);
    if (exit_code != 0) {
      switch (exit_code) {
        case 1: {
          fprintf(stderr,
                  "Error: Fail to open encrypt key file: %s, errno:%d.\n",
                  opt_clone_decrypt_key, errno);
        } break;

        case 2: {
          fprintf(stderr, "Error: Cannot malloc memory.\n");
        } break;

        case 3: {
          fprintf(stderr, "Error: Invalid dump encrypt key file.\n");
        } break;

        case 4: {
          fprintf(stderr, "Error: Fail to find encrypt mode.\n");
        } break;
        default:
          break;
      }
      free_resources();
      exit(EX_MYSQLERR);
    }

    if (opt_clone_decrypt_dir != nullptr) {
      cli_process_datadir(
          opt_clone_decrypt_dir, ".xbcrypt",
          [&](const datadir_entry_t &entry,
              void *arg [[maybe_unused]]) mutable -> bool {
            md_result_file = fopen(entry.clear_path.c_str(), "w");
            decrypt_clone_file(md_result_file, entry.path.c_str(),
                               clone_encrypt_mode, clone_encrypt_key,
                               clone_encrypt_iv);
            if (md_result_file) my_fclose(md_result_file, MYF(0));

            if (opt_remove_original) {
              printf("removing %s \n", entry.path.c_str());
              if (my_delete(entry.path.c_str(), MYF(MY_WME)) != 0) {
                return (false);
              }
            }
            return true;
          },
          nullptr);
    } else {
      decrypt_clone_file(md_result_file, opt_clone_decrypt_file,
                         clone_encrypt_mode, clone_encrypt_key,
                         clone_encrypt_iv);
      if (md_result_file) my_fclose(md_result_file, MYF(0));
    }

    free_resources();
    exit(0);
  }

  if (stderror_file) fclose(stderror_file);
  return (0);
} /* main */

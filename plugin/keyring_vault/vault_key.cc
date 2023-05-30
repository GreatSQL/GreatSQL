/* Copyright (c) 2018, 2021 Percona LLC and/or its affiliates. All rights
   reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "vault_key.h"
#include <string.h>
#include <sstream>

namespace keyring {

bool Vault_key::get_next_key(IKey **key_out) {
  if (was_key_retrieved) {
    *key_out = nullptr;
    return true;
  }
  *key_out = new Vault_key(*this);
  was_key_retrieved = true;
  return false;
}

bool Vault_key::has_next_key() { return !was_key_retrieved; }

void Vault_key::xor_data() { /* We do not xor data in keyring_vault */
}

void Vault_key::xor_data(uchar *,
                         size_t) { /* We do not xor data in keyring_vault */
}

void xor_data(uchar *, size_t) { /* We do not xor data in keyring_vault */
}

uchar *Vault_key::get_key_data() const { return key.get(); }

size_t Vault_key::get_key_data_size() const { return key_len; }

Key_type Vault_key::get_key_type() const {
  if (strcasecmp(key_type.c_str(), "AES") == 0) return Key_type::aes;
  if (strcasecmp(key_type.c_str(), "DSA") == 0) return Key_type::dsa;
  if (strcasecmp(key_type.c_str(), "SECRET") == 0) return Key_type::secret;
  return Key_type::unknown;
}

std::string *Vault_key::get_key_type_as_string() { return &this->key_type; }

const std::string *Vault_key::get_key_type_as_string() const {
  return &this->key_type;
}

void Vault_key::create_key_signature() const {
  if (key_id.empty()) return;
  std::ostringstream key_signature_ss;
  key_signature_ss << key_id.length() << '_';
  key_signature_ss << key_id;
  key_signature_ss << user_id.length() << '_';
  key_signature_ss << user_id;
  key_signature = key_signature_ss.str();
}

}  // namespace keyring

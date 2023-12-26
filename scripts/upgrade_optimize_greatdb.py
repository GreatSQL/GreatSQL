# Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
# 
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

import pymysql
import configparser
import sys


all_real_innodb_instant_tables = set()
optimize_failed_instant_tables = set()


def process_string(txt):
	s = txt;

	if '@' in s:
		s = s.replace('@', '@0040');

	for c in txt:
		if c=='!':
			s = s.replace(c, '@0021');
		elif c=='"':
			s = s.replace(c, '@0022');
		elif c=='#':
			s = s.replace(c, '@0023');
		elif c=='$':
			s = s.replace(c, '@0024');
		elif c=='%':
			s = s.replace(c, '@0025');
		elif c=='&':
			s = s.replace(c, '@0026');
		elif c=='\'':
			s = s.replace(c, '@0027');
		elif c=='(':
			s = s.replace(c, '@0028');
		elif c==')':
			s = s.replace(c, '@0029');
		elif c=='*':
			s = s.replace(c, '@002a');
		elif c=='+':
			s = s.replace(c, '@002b');
		elif c==',':
			s = s.replace(c, '@002c');
		elif c=='-':
			s = s.replace(c, '@002d');
		elif c=='.':
			s = s.replace(c, '@002e');
		elif c=='/':
			s = s.replace(c, '@002f');
		elif c==':':
			s = s.replace(c, '@003a');
		elif c==';':
			s = s.replace(c, '@003b');
		elif c=='<':
			s = s.replace(c, '@003c');
		elif c=='=':
			s = s.replace(c, '@003d');
		elif c=='>':
			s = s.replace(c, '@003e');
		elif c=='?':
			s = s.replace(c, '@003f');
		elif c=='{':
			s = s.replace(c, '@007b');
		elif c=='|':
			s = s.replace(c, '@007c');
		elif c=='}':
			s = s.replace(c, '@007d');
		elif c=='~':
			s = s.replace(c, '@007e');
		elif c=='[':
			s = s.replace(c, '@005b');
		elif '\\' == c:
			s = s.replace(c, '@005c');
		elif c==']':
			s = s.replace(c, '@005d');
		elif c=='^':
			s = s.replace(c, '@005e');
		elif c=='`':
			s = s.replace(c, '@0060');

	return s;


def proccess_special_db_table_name(i_db, i_table):
	i_changed_db = process_string(i_db)
	i_changed_table = process_string(i_table)

	i_path_db_table = i_changed_db + '/' + i_changed_table
	return i_path_db_table


def check_support_db(i_db):
	if i_db == 'information_schema' or i_db == 'performance_schema' or i_db == 'sys':
		return True
	else:
		return False


def usage():
	print('Configure:')
	print("conf_greatdb.ini needs to be placed in the same directory as the current python script file.")
	print("Before executing this script file, you need to create the conf_greatdb.ini file and configure the user, password, port and host.")
	print("an example of the conf_greatdb.ini file as follows:")
	print('[config]')
	print('user=root')
	print('password=')
	print('port=3306')
	print('host=127.0.0.1')
	print()
	print('Command:')
	print('python3 upgrade_optimize_greatdb.py 0 : display all instant tables')
	print('python3 upgrade_optimize_greatdb.py 1 : optimize all instant tables')
	print()


def parse_config():
	config = configparser.ConfigParser()

	res = [];
	try:
		res = config.read('conf_greatdb.ini')
	except Exception as e:
		usage()
		sys.exit(-1)
	else:
		if res:
			print()
		else:
			usage()
			sys.exit(-1)

	user = config['config']['user']
	password = config['config']['password']
	port = int(config['config']['port'])
	host = config['config']['host']

	return user, password, port, host


def display_all_instant_tables():
	print()
	print("All instant tables as follows :\n");
	for instant_tables in all_real_innodb_instant_tables:
		print(instant_tables)
	print()


def optimize_all_instant_tables():
	optimize_result = False
	
	usr, psswd, pt, ht = parse_config()
	connection = pymysql.connect(user=usr, password=psswd, port=pt, host=ht, database='')
	cursor = connection.cursor()

	print("Optimize instant tables :\n");
	for instant_tables in all_real_innodb_instant_tables:
		sql = "optimize table " + instant_tables
		print(sql)
		cursor.execute(sql)
		result = cursor.fetchone()
		res = result[2];

		if res == 'Error':
			optimize_result = True;
			optimize_failed_instant_tables.add(instant_tables)

	print()

	cursor.close()
	connection.close()

	return optimize_result


def get_instant_tables(cursor):
	# clear
	all_real_innodb_instant_tables.clear()

	cursor.execute("select TABLE_SCHEMA, TABLE_NAME from information_schema.TABLES where ENGINE = 'InnoDB'")
	all_innodb_tables = cursor.fetchall()

	# all tables which contain instant column
	cursor.execute("SELECT name FROM information_schema.innodb_tables where instant_cols > 0")
	all_instant_tables_path = cursor.fetchall()

	for db_table in all_innodb_tables:
		i_db = db_table[0];

		if check_support_db(i_db):
			continue

		i_table = db_table[1]
		i_path_db_table = proccess_special_db_table_name(i_db, i_table);

		for instant_table_path in all_instant_tables_path:
			instant_path = str(instant_table_path[0])

			# partition table
			if instant_path.find("#p#"):
				partition_path = instant_path.split('#')
				instant_path = partition_path[0]

			if instant_path == i_path_db_table:
				if '`' in i_db or '`' in i_table:
					if '`' in i_db:
						i_optimize_db = i_db.replace('`', '``');
					else:
						i_optimize_db = i_db;

					if '`' in i_table:
						i_optimize_table = i_table.replace('`', '``');
					else:
						i_optimize_table = i_table;

					all_real_innodb_instant_tables.add('`' + i_optimize_db + '`' + '.' + '`' + i_optimize_table + '`')
				else:
					all_real_innodb_instant_tables.add('`' + i_db + '`' + '.' + '`' + i_table + '`')


def get_all_instant_tables():
	usr, psswd, pt, ht = parse_config()

	print('the config is :')
	print('user:', usr)
	print('password:', psswd)
	print('port:', pt)
	print('host:', ht)
	print()

	connection = pymysql.connect(user=usr, password=psswd, port=pt, host=ht, database='')
	cursor = connection.cursor()

	# check version
	cursor.execute("SELECT @@version")
	all_verison = cursor.fetchall()
	mysql_version = "%s" % tuple(all_verison[0])
	mysql_version = mysql_version.split('-');
	version = mysql_version[0]
	if version > '8.0.25':
		print('The version of MySQL is greater than 8.0.25, so skip this verification of instant_cols in information_schema.innodb_tables');
		cursor.close()
		connection.close()
		return True;

	# instant tables
	get_instant_tables(cursor);
	
	cursor.close()
	connection.close()

	return False;


def main():
	if len(sys.argv) > 1:
		if (2 == len(sys.argv)) and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
			usage()
			sys.exit(0)
		elif (2 == len(sys.argv)) and (sys.argv[1] == '0'):
			res = get_all_instant_tables()
			if res == False:
				display_all_instant_tables()
		elif (2 == len(sys.argv)) and (sys.argv[1] == '1'):
			res = get_all_instant_tables()
			if res == False:
				optimize_result = optimize_all_instant_tables()

				if optimize_result == False:
					print()
					print()
					print('SUCCESSS : all instant column is eliminated')
					print()
				else:
					print()
					print("Failed optimize tables :");

					for fail_instant_tables in optimize_failed_instant_tables:
						print (fail_instant_tables)

		else:
			print('Invalid input parameter, the parameter is --help/-h or 0 or 1')
			print('python3 upgrade_optimize_greatdb.py 0 : display all instant tables')
			print('python3 upgrade_optimize_greatdb.py 1 : optimize all instant tables')

if __name__ == "__main__":
	main()


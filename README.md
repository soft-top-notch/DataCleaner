# Utilities to parse csv and SQL files

## Installation

Setup python packages in virtualenv for isolating from global packages:

```sh
$ sudo apt install python-virtualenv
$ virtualenv --python=python2 venv
$ ./venv/bin/pip install -r requirements.txt
```

## Using

Move SQL dump file to data directory and use short name:

```sh
$ mkdir -p data
$ mv DUMP_NAME.sql data/dump.sql
```

### Fix dump encoding

Sometimes SQL dump has wrong encoding and need convert it. For example for Russian encoding.

```sh
$ iconv -c -t latin1 data/cc_forum_fulldump.sql | iconv -c -f cp1251 -t utf-8 -o data/cc_forum_fulldump_utf8.sql
```

### Convert SQL dump to CSV files

```sh
$ ./venv/bin/python dbtools/sql_to_csv.py data/dump.sql
```
It created many CSV files in data directory (one file for table).

### Convert big SQL dump to CSV files

Sometimes SQL dump is too big and better convert only selected tables.

```sh
$ grep "^CREATE" data/big-dump.sql
$ ./venv/bin/python dbtools/sql_to_csv.py --tables='forum|pm|pmreceipt|pmtext|user' data/big-dump.sql
```

### Convert PostgreSQL dump to CSV files

PostgreSQL dump usually uses utf-8 encoding.

```sh
$ ./venv/bin/python dbtools/sql_to_csv.py --encoding=utf-8 data/postgres-dump.sql
```

### Merge user names to recipients

```sh
$ ./venv/bin/python dbtools/merge_user.py data/dump.hell_members.csv data/dump.hell_pm_recipients.csv
```

It created the new file data/dump.hell_pm_recipients.merged.csv with additional column.

### Convert private messages to JSON

```sh
$ ./venv/bin/python dbtools/pm_to_json.py --recipients=data/dump.hell_pm_recipients.merged.csv data/dump.hell_personal_messages.csv
```

It created the new file data/dump.hell_personal_messages.json.

### Convert forum messages to JSON

```sh
$ ./venv/bin/python dbtools/msg_to_json.py --forum='Hell' data/dump.hell_messages.csv
```

It created the new file data/dump.hell_messages.json.

### Show all tables

```sh
$ ./venv/bin/python dbtool.py --showtables data/mysql.sql
```

It print all tables of target sql dump.

### Convert sql dump table to csv file
```sh
$ ./venv/bin/python dbtool.py --extract data/mysql.sql --tables=table_1,table_2,table_3
```

It export selected tables into csv file

### Merge json after -j into big json
```sh
$ ./venv/bin/python datacleaner.py -jm [/path/to/source_folder] [/path/to/destination_folder]
```

- It scan for all json in source folder and merge json with same release into destination folder
- If only source folder provided, desitnation folder would be source_folder/merged_json
- If nothing provided, source folder is current running directory and destination folder is source_folder/merged_json


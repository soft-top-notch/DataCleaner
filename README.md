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

### Convert SQL dump to CSV files

```sh
$ ./venv/bin/python dbtools/sql_to_csv.py data/dump.sql
```
It created many CSV files in data directory (one file for table).

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
$ ./venv/bin/python dbtools/msg_to_json.py --forums=data/dump.hell_boards.csv data/dump.hell_messages.csv
```

It created the new file data/dump.hell_messages.json.

#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
#

from pymysqlreplication import BinLogStreamReader

MYSQL_SETTINGS = {
    "host": "rds-mysql-kinesis.cuiaej9siq22.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "user": "admin",
    "passwd": "ATPG2daT"
}


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                blocking=True)

    for binlogevent in stream:
        binlogevent.dump()

    stream.close()


if __name__ == "__main__":
    main()

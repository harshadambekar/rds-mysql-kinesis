import time
import json
import decimal
from datetime import datetime

import boto3

from pymysqlreplication import BinLogStreamReader

from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)



MYSQL_SETTINGS = {
    "host": "rds-mysql-kinesis.cuiaej9siq22.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "user": "admin",
    "passwd": "ATPG2daT"
}

def JSONEncoder_newdefault(kind=['uuid', 'datetime', 'time', 'decimal']):
    '''
    JSON Encoder newdfeault is a wrapper capable of encoding several kinds
    Use it anywhere on your code to make the full system to work with this defaults:
        JSONEncoder_newdefault()  # for everything
        JSONEncoder_newdefault(['decimal'])  # only for Decimal
    '''
    JSONEncoder_olddefault = json.JSONEncoder.default

    def JSONEncoder_wrapped(self, o):
        '''
        json.JSONEncoder.default = JSONEncoder_newdefault
        '''
        if ('datetime' in kind) and isinstance(o, datetime):
            return str(o)
        if ('time' in kind) and isinstance(o, time.struct_time):
            return datetime.fromtimestamp(time.mktime(o))
        if ('decimal' in kind) and isinstance(o, decimal.Decimal):
            return str(o)
        return JSONEncoder_olddefault(self, o)
    json.JSONEncoder.default = JSONEncoder_wrapped

def main():

  JSONEncoder_newdefault()
  kinesis = boto3.client("kinesis")

  stream = BinLogStreamReader(connection_settings= MYSQL_SETTINGS,
                              server_id=100,
                              blocking=True,
                              resume_stream=True,
                              only_schemas="rds_mysql_kinesis",
                              only_tables="employee"
                              ,only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
                              )

  for binlogevent in stream:
    print(binlogevent.dump())
    
    for row in binlogevent.rows:      
      event = {"schema": binlogevent.schema,
                "table": binlogevent.table,
                "type": type(binlogevent).__name__,
                "row": row
              }      
    kinesis.put_record(StreamName="mysql-kinesis-ingest", Data=json.dumps(event), PartitionKey="default")    

def lambda_handler (event, contxt):
   main()
   return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
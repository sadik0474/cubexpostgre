import cx_Oracle
import psycopg2
import sys
from pprint import pprint
from collections import namedtuple

conn = psycopg2.connect(host="juniper-pg.postgres.database.azure.com", database="cubex_test",
                        user="juniper_admin@juniper-pg.postgres", password="Info1234")
cur = conn.cursor()


class OraConn(object):
    def __int__(self, host_details):
        self.result = None
        self._host = host_details['host']
        self._port = host_details['port']
        self._uname = host_details['uname']
        self._password = host_details['upassword']
        self._sid = host_details['sid']

    def open_conn(self):
        dsn_tns = cx_Oracle.makedsn(self._host, self._port, )
        self.ora_conn = cx_Oracle.connect(user=self._host, password=self._password, dsn=dsn_tns)

    def close_conn(self):
        self.ora_conn.close()

    def select_op(self, query):
        self.ora_cur = self.ora_conn.cursor()
        try:
            self.ora_cur.execute(query)
            self.result = self.ora_cur.fetchall()
            return self.result
        except Exception as e:
            print(e)
            self.close_conn()
            sys.exit(1)
        finally:
            self.ora_cur.close()

    def hip_inserts(self, rows):
        try:
            self.insert_cur = self.ora_conn.cursor()
            self.insert_cur.bindarraysize = len(rows)
            pprint(str(rows))
            self.insert_cur.setinputsizes(100, cx_Oracle.DATETIME, 100, 100, 100)
            self.insert_cur.exectemany(
                """insert into logger_stats_master(event_feed_id,event_batch_date,event_run_id,event_type,event_value) 
                values (:1,:2,:3,:4,:5)"""
                , rows
            )
        except Exception as e:
            print(e)
            self.ora_conn.rollback()
            self.close_conn()
            sys.exit(1)
        finally:
            self.insert_cur.close()


class PgCon():
    def __init__(self, host_details):
        # host, port, uname, upassword, dbname
        self.pg_conn = None
        self._host = host_details['host']
        self._port = host_details['port']
        self._uname = host_details['uname']
        self._upassword = host_details['upassword']
        self._dbname = host_details['dbname']

    def open_conn(self):
        self.pg_conn = psycopg2.connect(host=self._host, port=self._port,
                                        database=self._dbname,
                                        user=self._uname, password=self._upassword)

    def close_conn(self):
        self.pg_conn.close()

    def sel_query(self, query):
        try:
            self.sel_cur = self.pg_conn.cursor()
            self.sel_cur.execeute(query)
            self.result = self.sel_cur.fetchall()
            return self.result
        except Exception as e:
            print(e)
            self.close_conn()
            sys.exit(1)
        finally:
            self.sel_cur.close()

    def bulk_insert(self, filename, tablename):
        return_tuple = namedtuple("pg_count", ["tablename", "count"])
        bulk_cur = self.pg_conn.cursor()
        sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '^'"

        try:
            pprint("File Bulk load :{} for Table :{}".format(filename, tablename))
            with open(filename, 'r') as fi:
                bulk_cur.copy_expert(sql=sql % tablename, file=fi)
            return return_tuple(tablename, bulk_cur.rowcount)

        except Exception as e:
            print("Exiting the code for the table :{} as File {} copy failed....".format(filename, tablename))
            print(e)
            self.pg_conn.rollback()
            sys.exit(1)

        finally:
            bulk_cur.close()
        self.pg_conn.commit()

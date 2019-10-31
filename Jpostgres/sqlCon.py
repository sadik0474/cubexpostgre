import cx_Oracle
import psycopg2
import sys
from collections import namedtuple
import logging
import subprocess
#todo remove pg_conn.commit() and add
# -create table entries for status
# - restartability

# todo logging when failure happened

class JLogger:
    def __init__(self):
        logging.basicConfig(format = '%(asctime)s-%(levelname)s-%(message)s', datefmt='%d-%b-%y:%H:%M:%S')
        #self.formatter = logging.Formatter
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)


class PgCon(JLogger):
    def __init__(self, host_details):
        JLogger.__init__(self)
        #self.logger.info("Received Param : {}".format(host_details))
        # host, port, uname, upassword, dbname
        self.pg_conn = None
        self._host = host_details['host']
        self._port = host_details['port']
        self._uname = host_details['uname']
        self._upassword = host_details['upassword']
        self._dbname = host_details['dbname']

    def open_conn(self):
        try:
            self.logger.info('Opening  Conn ...')
            self.pg_conn = psycopg2.connect(host=self._host, port=self._port,
                                            database=self._dbname,
                                            user=self._uname, password=self._upassword)
            self.logger.info(' Connection Successful ...')
        except Exception as e:
            self.logger.exception("Failed to create connection {} ".format(e))
            #self.logger.info("{} ".format(e.diag.message_detail))
            sys.exit(1)

    def close_conn(self):
        self.logger.info('Closing Target Conn ...')
        self.pg_conn.close()

    def insert(self, table, cols, ins_tuple):
        try:
            cols=cols
            table = table
            self.logger.info(table)
            self.logger.info(cols)
            self.ins = self.pg_conn.cursor()
            self.logger.info(', '.join(['%s'] * len(ins_tuple)))
            query = self.ins.mogrify("INSERT INTO {} ({}) VALUES {} ".format(
                table,
                cols,
                ', '.join(['%s'] * len(ins_tuple))), ins_tuple)
            self.logger.info("query : {} ".format(query))
            self.ins.execute(query)
            self.pg_conn.commit()

        except Exception as e:
            self.logger.exception("Failed to insert {} ".format(e))
            #self.logger.info("{} ".format(e.diag.message_detail))

    def sel_query(self, query):
        try:
            self.sel_cur = self.pg_conn.cursor()
            self.logger.info('query : {}'.format(query))
            self.sel_cur.execute(query)
            self.result = self.sel_cur.fetchall()
            return self.result
        except Exception as e:
            self.logger.exception('Excecption Occured in PG select : {}'.format(e))
            self.close_conn()
            sys.exit(1)
        finally:
            self.sel_cur.close()

    def bulk_copy(self):
        """PGPASSWORD={} psql -h {} -U {}  -d cubex -c "\copy JUNIPER_METADATA.JUNIPER_EXT_FEED_MASTER from
         /data/juniper/in/IN/dadasda/20191025/157202775900012/data/dadasda_JUNIPER_METADATA.JUNIPER_EXT_FEED_MASTER_157202775900012_e597edec-2e01-4b48-baeb-6bb2e3907850_2019-10-25-18-28-08-496.csv
         WITH CSV HEADER DELIMITER AS ',' " """
        subprocess.call()

    def bulk_insert_query(self,header,delimiter,tablename):
        if not header :
            sql = "COPY {} FROM STDIN WITH CSV DELIMITER AS '{}'".format(tablename, delimiter)
            self.logger.info(sql)

        else:
            sql = "COPY {} FROM STDIN WITH CSV HEADER DELIMITER AS '{}'".format(tablename, delimiter)
            self.logger.info("SQL getting loaded : {}".format(sql))
        return sql

    def bulk_insert(self, filename, tablename, header=False, delimiter=','):
        self.logger.info("Bulk insert to Postgres Target is initiated ...\n File Name : {} \n Table Name : {}".
                         format(filename,tablename))
        return_tuple = namedtuple("pg_count", ["tablename", "count"])
        bulk_cur = self.pg_conn.cursor()
        sql = self.bulk_insert_query(header, delimiter,tablename)#"COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '^'"

        try:
            #self.logger.info("File Bulk load :{} \n Table :{}".format(filename, tablename))
            with open(filename, 'r') as fi:
                bulk_cur.copy_expert(sql=sql, file=fi)
            self.logger.info("{}".format(return_tuple(tablename, bulk_cur.rowcount)))
            return return_tuple(tablename, bulk_cur.rowcount)

        except Exception as e:
            self.logger.info("File {} copy failed \n Roll back initiated for the  table :{} ".format(filename, tablename))
            self.logger.info(e)
            return 'Failed'
            #self.pg_conn.rollback()
            #sys.exit(1)

        finally:
            pass
            #bulk_cur.close()
            #self.pg_conn.commit()

    def hip_inserts(self, rows):
        try:
            if not self.pg_conn:
                self.open_conn()

            self.insert_cur = self.pg_conn.cursor()
            # self.insert_cur.bindarraysize = len(rows)
            self.logger.info(str(rows))
            # self.insert_cur.setinputsizes(100, cx_Oracle.DATETIME, 100, 100, 100)
            self.insert_cur.executemany(
                """insert into juniperx.logger_stats_master(event_feed_id,event_batch_date,event_run_id,event_type,event_value) 
                values (:1,:2,:3,:4,:5)"""
                , rows
            )
        except Exception as e:
            self.logger.info(e)
            self.pg_conn.rollback()
            self.close_conn()
            # sys.exit(1)
        finally:
            self.insert_cur.close()

if __name__=='__main__':
    PgCon()
    [('cubex_test',
      datetime.datetime(2019, 10, 15, 0, 0),
      '15710976000000128',
      'PG_DEMO~JUNIPER_METADATA.JUNIPER_PROJECT_MASTER-count',
      6)]
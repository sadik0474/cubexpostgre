import os, sys
import re
import sqlalchemy
from glob import glob
from azure.sqlCon import OraConn, PgCon
from pprint import pprint
import datetime




class PGLoad(object):

    def __init__(self, meta_feed_id, meta_run_id, targetName):
        self.targetName=targetName
        self.feed_id = meta_feed_id
        self.run_id = meta_run_id
        self.feed_unique_name = None
        self.NIFI_sequence = None
        self.extracted_date = None
        self.counter_object = {}
        self.recon_tuple = list

    def recon(self):
        oc.open_conn()
        self.extracted_date = datetime.datetime.strptime(self.extracted_date, '%Y%m%d')
        self.recon_tuple = []
        for tableName, count in self.counter_object.items():
            self.recon_tuple.append((self.feed_unique_name, self.extracted_date, self.run_id + self.NIFI_sequence,
                                     "{}~{}-count".format(self.targetName, tableName, count)))
        pprint(self.recon_tuple)
        oc.hip_inserts(self.recon_tuple)

    def call_pgbulk(self, each_record):

        self.feed_unique_name = each_record[0]
        table_name = each_record[1]
        country_code = each_record[2]
        self.NIFI_sequence = str(each_record[3])
        self.extracted_date = each_record[4]
        path = '{}/{}/{}/{}/{}/data/{}_{}_{}_*'.format(blob, country_code, self.feed_unique_name, self.extracted_date,
                                                       self.run_id, self.feed_unique_name, table_name, self.run_id)
        pprint(path)
        files = glob(path)
        pprint(files)
        for each_file in files:
            file_copy = pc.bulk_insert(each_file, table_name)
            self.counter_object[file_copy.tableName] = self.counter_object.get(file_copy.tableName, 0) + file_copy.count

    def get_metadata(self):

        query = """select efm.feed_unique_name,etm.table_name,EFM.COUNTRY_CODE,ens.nifi_status_sequence,ens.extracted_date from juniper_ext_table_master etm join juniper_ext_feed_master  efm
    on etm.feed_sequence=efm.feed_sequence  join 
    juniper_ext_nifi_status ens on etm.feed_sequence=ens.feed_id
    where etm.feed_sequence={} and ens.run_id='{}' and ens.job_type='R'""".format(self.feed_id, self.run_id)

        oc.open_conn()
        pc.open_conn()
        meta_result = oc.select_op(query)
        oc.close_conn()
        pprint(meta_result)
        for each_record in meta_result:
            self.call_pgbulk(each_record)
        self.recon()


if __name__ == '__main__':
    feedId = 825
    runId = '157032037200001'
    targetName = 'hAzureDemo'
    oc = OraConn()
    pc = PgCon()
    pg_Obj = PGLoad(feedId, runId, targetName)
    pg_Obj.get_metadata()

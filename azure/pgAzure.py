from pprint import pprint
import datetime
from glob import glob
import os,sys
from azure.sqlCon import OraConn, PgCon

if sys.version_info[0] > 2:
    import configparser
else :
    import ConfigParser as configparser


class MetaDb(object):
    config = configparser.RawConfigParser()
    config.read('./pgcopy.cfg')
    meta_target_table = config.get('meta_tables', 'target')

    def __int__(self):
        self.meta_sect = dict(self.config.items('pg_meta_db'))
        self.meta_db_obj = PgCon(self.meta_sect)

    def get_meta_details(self,feed_id,run_id):
        return self.set_meta_details(feed_id,run_id)

    def get_target_details(self,target_name):
        return self.get_target_details(target_name)

    def set_meta_details(self, feed_id, run_id):
        self.meta_db_obj.open_conn()
        query = """select efm.feed_unique_name,etm.table_name,EFM.COUNTRY_CODE,ens.nifi_status_sequence,
            ens.extracted_date from juniper_ext_table_master etm join juniper_ext_feed_master  efm
            on etm.feed_sequence=efm.feed_sequence  join 
            juniper_ext_nifi_status ens on etm.feed_sequence=ens.feed_id
            where etm.feed_sequence={} and ens.run_id='{}' and ens.job_type='R'""".format(feed_id, run_id)
        meta_details = self.meta_db_obj.sel_query(query)
        return meta_details

    def set_target_details(self, target_name):
        target_details={}
        target_query ="""select ora_host_name,ora_port_no,ora_username,password,ora_database_name  
                            from {} 
                                where target_unique_name={}""".format(self.meta_target_table, target_name)
        result = self.meta_db_obj.sel_query(target_query)
        target_details['host'] = result[0]
        target_details['port'] = result[0]
        target_details['uname'] = result[0]
        target_details['upassword'] = result[0]
        target_details['dbname'] = result[0]

        return target_details


class PgLoad(object):

    def __int__(self,feedId,runid,targetName):
        self.feedId = feedId
        self.runid = runid
        self.targetName = targetName
        self.mb = MetaDb()
        self.blob = self.mb.config.get('target', 'blob_path')
        self.counter_object = {}
        self.recon_tuple = list

    def copy_tables(self, tables):
        for each_table in tables:
            table_name = each_table[1]
            country_code = each_table[2]
            path = '{}/{}/{}/{}/{}/data/{}_{}_{}_*'.format(self.blob, country_code, self.feed_unique_name,
                                                           self.extracted_date,
                                                           self.runid, self.feed_unique_name, table_name, self.runid)
            files = glob(path)
            for each_file in files:
                file_copy = self.target_db_obj.bulk_insert(each_file, table_name)
                self.counter_object[file_copy.tableName] = self.counter_object.get(file_copy.tableName,
                                                                                   0) + file_copy.count

    def recon(self):
        self.mb.meta_db_obj.open_conn()
        self.extracted_date = datetime.datetime.strptime(self.extracted_date, '%Y%m%d')
        self.recon_tuple = []
        for tableName, count in self.counter_object.items():
            self.recon_tuple.append((self.feed_unique_name, self.extracted_date, self.runid + self.NIFI_sequence,
                                     "{}~{}-count".format(self.targetName, tableName, count)))
        pprint(self.recon_tuple)
        self.mb.meta_db_obj.hip_inserts(self.recon_tuple)

    def load_process(self):
        source_tables = self.mb.get_meta_details(self.feedId, self.runid)
        self.feed_unique_name = source_tables[0][0]
        self.NIFI_sequence = str(source_tables[0][3])
        self.extracted_date = source_tables[0][4]
        target_host_details = self.mb.get_target_details(self.targetName)
        self.mb.meta_db_obj.close_conn()

        self.target_db_obj = PgCon(target_host_details)
        self.target_db_obj.open_conn()
        self.copy_tables(source_tables)


if __name__ == '__main__':
    feedId = 825
    run_id = '157032037200001'
    targetName = 'hAzureDemo'

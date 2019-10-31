class OraConn(JLogger):
    def __init__(self, host_details):
        JLogger.__init__()
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
            self.logger.info(e)
            self.close_conn()
            #sys.exit(1)
        finally:
            self.ora_cur.close()



    def open_conn(self):
        try:
            self.logger.info('Opening Target Conn ...')
            self.pg_conn = psycopg2.connect(host=self._host, port=self._port,
                                            database=self._dbname,
                                            user=self._uname, password=self._upassword)
            self.logger.info('Target Connection Successful ...')
        except psycopg2.Error as e:
            self.logger.info("Failed to insert {} ".format(e.pgerror))
            self.logger.info("{} ".format(e.diag.message_detail))

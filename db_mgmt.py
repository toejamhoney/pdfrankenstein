import os
import sys
import sqlite3

import cfg


class DBGateway(object):

    def __init__(self, dbtype=''):
        self.cfg = cfg.Config()
        if dbtype is 'test':
            self.db_dir = os.getcwd()
            self.db_name = 'testdb.sqlite'
        else:
            self.db_dir = self.cfg.setting('database', 'path')
            self.db_name = self.cfg.setting('database', 'db')
        if not (self.db_dir and self.db_name):
            print 'Error in database path or name. Check cfg file'
            sys.exit(1)
        db_path = os.path.join(self.db_dir, self.db_name)
        print 'Using:', db_path
        self.db_conn = sqlite3.connect(db_path)
        self.db_conn.text_factory = str
        self.db_curr = self.db_conn.cursor()

    def attach(self, db_name):
        db = "'" + os.path.join(config.SETTINGS.get('DB_DIR'), db_name) + "'"
        self.db_curr.execute('ATTACH DATABASE ' + db + ' AS ' + db_name)
        self.db_conn.commit()

    def create_table(self, table, **kwargs):
        try:
            kwargs = self.format_args(**kwargs)
            cmd = 'CREATE TABLE IF NOT EXISTS ' + table
            if kwargs.get('select'):
                cmd += ' AS SELECT ' + kwargs.get('select') + ' FROM ' + kwargs.get('from') + ' WHERE ' + kwargs.get('where') + '=' + kwargs.get('is')
            else:
                cmd += ' (' + kwargs.get('cols') + ', PRIMARY KEY(' + kwargs.get('primary') + '))'
        except TypeError as e:
            print 'Invalid arguments passed to database gateway:', kwargs
            raise e
        else:
            try:
                self.db_curr.execute(cmd)
            except sqlite3.OperationalError as error:
                print 'Invalid operation in database gateway:', error
                raise error
            else:
                self.db_conn.commit()
                #self.dump()

    def disconnect(self):
        self.db_conn.commit()
        self.db_conn.close()

    def drop_tables(self):
        self.db_curr.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for row in self.db_curr.fetchall():
            self.drop(row[0])

    def drop(self, name):
        self.db_curr.execute("DROP TABLE IF EXISTS " + name)
        self.db_conn.commit()

    def format_args(self, **kwargs):
        if isinstance(kwargs.get('primary'), (tuple, list)):
            kwargs['primary'] = ', '.join(kwargs['primary'])
        if isinstance(kwargs.get('cols'), (tuple, list)):
            kwargs['subs'] = ', '.join( ['?' for arg in kwargs['cols']] )
            kwargs['cols'] = ', '.join(kwargs['cols'])
        else:
            kwargs['subs'] = '?'
        return kwargs
    
    def insert(self, table, **kwargs):
        kwargs = self.format_args(**kwargs)
        cmd = 'INSERT INTO ' + table + '(' + kwargs.get('cols') + ') VALUES (' + kwargs.get('subs') + ')'
        try:
            self.db_curr.execute(cmd, kwargs.get('vals'))
        except sqlite3.IntegrityError as e:
            if e.message.startswith('UNIQUE'):
                pass
            else:
                print e
        self.db_conn.commit()

    def select(self, cmd_str):
        cmd = 'SELECT %s' % cmd_str
        self.db_curr.execute(cmd)
        return self.db_curr.fetchall()

    def count(self, table):
        cmd = "SELECT COUNT (*) FROM " + table
        self.db_curr.execute(cmd)
        return self.db_curr.fetchone()[0]

    def update(self, **kwargs):
        kwargs = self.format_args(**kwargs)
        cmd = 'UPDATE :table SET :col = :val WHERE :key = :kval'
        self.db_curr.execute(cmd, kwargs)
        self.db_conn.commit()

    def delete(self, *ids):
        pass

    def dump(self, n=0):
        print ':MEMORY DB DUMP:'
	cnt = 0
        for val in self.db_conn.iterdump():
            cnt += 1
            if n > 0 and cnt >= n:
		break
            print val
        print ':MEMORY DB DUMP END:'

# Testing
if __name__ == "__main__":
    import threading
    num_threads = 10
    gw = DBGateway(sys.argv[1])
    print gw.count('parsed_pdfs')
    errors = gw.select("pdfmd5, tree FROM parsed_pdfs WHERE treemd5='' or treemd5 is null")
    for err in errors:
        print err
    '''
    table = 'test_table'

    print '-'*20, 'Test Full Iterables', '-'*20
    col = [ "id INT", "date DATETIME", "name TEXT" ]
    prime = ('id', 'name')
    try:
        gw.create_table(table, cols=col, primary=prime)
    except Exception as error:
        repr(error)

    print '-'*20, 'Test Singles', '-'*20
    cols = 'id INT'
    primary = 'id'
    kwargs = { 'cols':cols, 'primary':primary }
    try:
        gw.create_table(table, **kwargs)
    except Exception as error:
        print repr(error)

    print '-'*20, 'Test Nones', '-'*20
    cols = None
    primary = None
    kwargs = { 'cols':cols, 'primary':primary }
    try:
        gw.create_table(table, **kwargs)
    except Exception as error:
        print repr(error)

    print '-'*20, 'Test Mix', '-'*20
    cols = ('id INT', None)
    primary = 'id'
    kwargs = { 'cols':cols, 'primary':primary }
    try:
        gw.create_table(table, **kwargs)
    except Exception as error:
        print repr(error)

    print '-'*20, 'Test Threads', '-'*20
    table = 'thread_table'
    cols = ('thread_id INT', 'value TEXT')
    primary = 'thread_id'
    gw.drop(table)
    gw.create_table(table, cols=cols, primary=primary)
    gw.insert(table, cols='thread_id', vals=('all',))

    def work(i):
        db_gw = DBGateway('test')
        stuff = str(i)*100
        db_gw.update(table='thread_table', col='value', val=stuff, key='thread_id', kval='all')

    threads = []
    for i in range(num_threads):
        thr = threading.Thread(target=work, args=(i,))
        threads.append(thr)
        thr.start()

    for thread in threads:
        thread.join()

    '''

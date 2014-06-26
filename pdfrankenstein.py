import io
import os
import sys
import glob
import time
import getopt
import hashlib
import traceback
import multiprocessing
from Queue import Full, Empty

from  peepdf.PDFCore import PDFParser

LOCK = multiprocessing.Lock()
STORAGE_LOCK = multiprocessing.Lock()

class ParserFactory(object):

    def new_parser(self):
        parser = None
        try:
            parser = ArgParser()
        except ImportError:
            parser = GetOptParser()
        finally:
            return parser 

class ParsedArgs(object):
    '''
    This is the namespace for our parsed arguments to keep dot access.
    Otherwise we would create a dictionary with vars(args) in ArgParser,
    or manually in GetOptParser. (6 and 1/2 dozen of the other.)

    Defaults set here for GetOpt's shortcomings.
    '''
    pdf_in = None
    out = 't-hash-{stamp}.txt'.format(stamp = time.strftime("%Y-%m-%d_%H-%M-%S"))
    debug = False
    verbose = False

class ArgParser(object):

    def __init__(self):
	import argparse
        if not argparse:
            print 'Error in ArgParser. Unable to import argparse'
            sys.exit(1)
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('pdf_in', help="PDF input for analysis")
        self.parser.add_argument('-o', '--out', default='t-hash-'+time.strftime("%Y-%m-%d_%H-%M-%S")+'.txt', help="Analysis output filename or type. Default to timestamped file in CWD. Options: 'db'||'stdout'||[filename]")
        self.parser.add_argument('-d', '--debug', action='store_true', default=False, help="Print debugging messages")
        self.parser.add_argument('-v', '--verbose', action='store_true', default=False, help="Spam the terminal")

    def parse(self):
        '''
        No need to pass anything; defaults to sys.argv (cli input)
        '''
        try:
            parsed = ParsedArgs()
            self.parser.parse_args(namespace=parsed)
        except Exception:
            self.parser.exit(status=0, message='Usage: pdfrankenstein.py <input pdf> [-o] [-d] [-v]\n')
        else:
            return parsed

class GetOptParser(object):
    '''
    Necessary for outdated versions of Python. Versions that aren't even
    updated, and won't even have src code security updates as of 2013.
    '''
    shorts = 'o:dv'
    longs = [ 'out=', 'debug', 'verbose' ]

    def parse(self):
        parsed = ParsedArgs()
        opts, remain = self._parse_cli()
        parsed.pdf_in = remain[0]
        for opt, arg in opts:
            if opt in ('-o', '--out'):
                '''
                GetOpt can't recognize the difference between a missing value
                for '-o' and the next flag: '-d', for example. This creates a
                file called '-d' as output, and rm can't remove it since that
                is a flag for rm.
                '''
                if arg[0].isalnum():
                    parsed.out = arg
                else:
                    print 'Invalid output name. Using default:', parsed.out
            elif opt in ('-d', '--debug'):
                parsed.debug = True
            elif opt in ('-v', '--verbose'):
                parsed.verbose = True
        return parsed 

    def _parse_cli(self):
        try:
            o, r = getopt.gnu_getopt(sys.argv[1:], self.shorts, self.longs)
        except IndexError:
            print 'Usage: pdfrankenstein.py <input pdf> [-o value] [-d] [-v]'
            sys.exit(1)
        else:
            if len(r) != 1:
                print 'One PDF file or directory path required'
                print 'Usage: pdfrankenstein.py <input pdf> [-o value] [-d] [-v]'
                sys.exit(1)
            return o, r

class Hasher(multiprocessing.Process):
    '''
    Hashers generally make hashes of things
    def __init__(self, qin, storage, counter):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.storage = StorageFactory().new_storage(storage)
        self.counter = counter

    def run(self):
        self.storage.open()
        proceed = True
        while proceed:
            t_hash = self.qin.get()
            if not t_hash:
                write('\nStasher: kill msg recvd\n')
                proceed = False
            else:
                self.storage.store(t_hash)
                self.counter.inc()
            self.qin.task_done()
        self.storage.close()
        write('\nStasher: Storage closed. Exiting.\n')
    '''
    def __init__(self, qin, qout, counter, storage):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.qout = qout
        self.counter = counter
        #self.storage = StorageFactory().new_storage(storage)

    def run(self):
        #self.storage.open()
        while True:
            pdf = self.qin.get()

            if not pdf:
                self.qin.task_done()
                return 0

            pdf_name = pdf.rstrip(os.path.sep).rpartition(os.path.sep)[2]
            rv, parsed_pdf = self.parse_pdf(pdf)

            if not rv:
                js = ''
                t_hash = ''
                t_str = parsed_pdf
            else:
                t_hash, t_str = self.get_tree_hash(parsed_pdf)
                jscript = self.get_js(parsed_pdf)
                swflash = self.get_swf(parsed_pdf)

            self.qout.put({'pdf_md5':pdf_name, 'tree_md5':t_hash, 'tree':t_str, 'obf_js':jscript, 'swf':swflash})
            #self.storage.store( (pdf_name, t_hash, t_str, js, swf) )
            self.counter.inc()
            self.qin.task_done()
        #self.storage.close()

    def parse_pdf(self, pdf):
        retval = True
	try:
            _, pdffile = PDFParser().parse(pdf, forceMode=True, manualAnalysis=True)
        except Exception as e:
            retval = False
            pdffile = '\n'.join([traceback.format_exc(), repr(e)])
        return retval, pdffile

    def get_swf(self, pdf):
        swf = ''
        for version in range(pdf.updates + 1):
            for idx, obj in pdf.body[version].objects.items():
                if obj.object.type == 'stream':
                    stream_ident = obj.object.decodedStream[:3]
                    if stream_ident in ['CWS', 'FWS']:
                        swf += obj.object.decodedStream.strip()
        return swf

    def get_js(self, pdf):
        js = ''
        for version in range(pdf.updates+1):
            for obj_id in pdf.body[version].getContainingJS():
                js += self.do_js_code(obj_id, pdf)
        return js

    def get_tree_hash(self, pdf):
        tree_string = self.do_tree(pdf)
        m = hashlib.md5()
        m.update(tree_string)
	tree_hash = m.hexdigest()
	if not tree_string:
            tree_string = 'Empty tree. Hash on empty string.'
        return tree_hash, tree_string

    def do_js_code(self, obj_id, pdf):
        consoleOutput = ''
        obj_id = int(obj_id)
        pdfobject = pdf.getObject(obj_id, None)
        if pdfobject.containsJS():
            jsCode = pdfobject.getJSCode()
            for js in jsCode:
                consoleOutput += js
        return consoleOutput

    def do_tree(self, pdfFile):
        version = None
        treeOutput = ''
        tree = pdfFile.getTree()
        for i in range(len(tree)):
            nodesPrinted = []
            root = tree[i][0]
            objectsInfo = tree[i][1]
            if i != 0:
                treeOutput += ' Version '+str(i)+': '
            if root != None:
                nodesPrinted, nodeOutput = self.printTreeNode(root, objectsInfo, nodesPrinted)
                treeOutput += nodeOutput
            for object in objectsInfo:
                nodesPrinted, nodeOutput = self.printTreeNode(object, objectsInfo, nodesPrinted)
                treeOutput += nodeOutput
        return treeOutput
            
    def printTreeNode(self, node, nodesInfo, expandedNodes = [], depth = 0, recursive = True):
        '''
            Given a tree prints the whole tree and its dependencies
            
            @param node: Root of the tree
            @param nodesInfo: Information abour the nodes of the tree
            @param expandedNodes: Already expanded nodes
            @param depth: Actual depth of the tree
            @param recursive: Boolean to specify if it's a recursive call or not
            @return: A tuple (expandedNodes,output), where expandedNodes is a list with the distinct nodes and output is the string representation of the tree
        '''
        output = ''
        if nodesInfo.has_key(node):
            if node not in expandedNodes or (node in expandedNodes and depth > 0):
                output += nodesInfo[node][0] + ' (' +str(node) + ') '
            if node not in expandedNodes:
                expandedNodes.append(node)
                children = nodesInfo[node][1]
                if children != []:
                    for child in children:
                        if nodesInfo.has_key(child):
                            childType = nodesInfo[child][0]
                        else:
                            childType = 'Unknown'
                        if childType != 'Unknown' and recursive:
                            expChildrenNodes, childrenOutput = self.printTreeNode(child, nodesInfo, expandedNodes, depth+1)
                            output += childrenOutput
                            expandedNodes = expChildrenNodes
                        else:
                            output += childType + ' (' +str(child) + ') '
                else:
                    return expandedNodes,output
        return expandedNodes,output



class Stasher(multiprocessing.Process):
    '''
    Stashers are the ant from the ant and the grashopper fable. They save
    things up for winter in persistent storage.
    '''
    def __init__(self, qin, storage, counter):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.storage = StorageFactory().new_storage(storage)
        self.counter = counter

    def run(self):
        self.storage.open()
        proceed = True
        while proceed:
            t_hash = self.qin.get()
            if not t_hash:
                write('\nStasher: kill msg recvd\n')
                proceed = False
            else:
                self.storage.store(t_hash)
                self.counter.inc()
            self.qin.task_done()
        self.storage.close()
        write('\nStasher: Storage closed. Exiting.\n')


class StorageFactory(object):

    def new_storage(self, typ):
        if typ == 'stdout':
            return StdoutStorage()
        if typ == 'db':
            return DbStorage()
        else:
            return FileStorage(typ)

class Storage(object):

    def __init__(self):
        pass
    def open(self):
        pass
    def store(self):
        pass
    def close(self):
        pass

class StdoutStorage(Storage):
    def __init__(self):
        pass
 
class DbStorage(Storage):
    
    from db_mgmt import DBGateway
    table = 'parsed_pdfs'
    cols = ( 'pdf_md5', 'tree_md5', 'tree', 'graph', 'obf_js', 'deobf_js', 'swf', 'abc', 'actionscript', 'shellcode', 'bin_blob' )
    primary = 'pdf_md5'
    
    def __init__(self):
        self.db = self.DBGateway()

    def open(self):
        self.db.create_table(self.table, cols=[ ' '.join([col, 'TEXT']) for col in self.cols], primary=self.primary)

    def store(self, data_list):
        data_list = self.align_kwargs(data_list)
        self.db.insert(self.table, cols=self.cols, vals=data_list)
    
    def close(self):
        self.db.disconnect()

    def align_kwargs(self, data):
        aligned = []
        for col in self.cols:
            aligned.append(data.get(col, ''))
        return tuple(aligned)


class FileStorage(Storage):

    def __init__(self, path):
        self.path = path
        try:
            self.fd = open(path, 'wb')
        except IOError as e:
            print e
            print 'Unable to create output. Exiting.'
            sys.exit(1)
        else:
            self.fd.close()

    def open(self):
        self.fd = open(self.path, 'wb')

    def store(self, data_list):
        try:
            self.fd.write('%s\n' % '\t'.join(data_list))
        except IOError as e:
            print e
            print 'Unable to write to output file.'
            sys.exit(1)

    def close(self):
        self.fd.close()

class Counter(object):

    def __init__(self, soft_max, name='Untitled'):
        self.counter = multiprocessing.RawValue('i', 0)
        self.hard_max = multiprocessing.RawValue('i', 0)
        self.soft_max = soft_max
        self.lock = multiprocessing.Lock()
        self.name = name

    def inc(self):
        with self.lock:
            self.counter.value += 1

    def value(self):
        with self.lock:
            return self.counter.value
    
    def complete(self):
        with self.lock:
            if self.hard_max > 0:
                return self.counter.value == self.hard_max.value

    def ceil(self):
        return self.hard_max.value


class Jobber(multiprocessing.Process):

    def __init__(self, job_list, job_qu, validator, counters, num_procs):
        multiprocessing.Process.__init__(self)
        self.jobs = job_list
        self.qu = job_qu
        self.qu.cancel_join_thread()
        self.counters = counters
        self.validator = validator
        self.num_procs = num_procs

    def run(self):
        write("Jobber started\n")
        job_cnt = 0
        for job in self.jobs:
            if self.validator.valid(job):
                self.qu.put(job)
                job_cnt += 1
        for n in range(self.num_procs):
            self.qu.put(None)
        for counter in self.counters:
            counter.hard_max.value = job_cnt
        write("Job queues complete. Counters set.\n")


class ProgressBar(multiprocessing.Process):

    def __init__(self, counters, io_lock, qu):
        multiprocessing.Process.__init__(self)
        self.counters = counters
        self.io_lock = io_lock
        self.msg_qu = qu

    def run(self):
        while any(not counter.ceil() for counter in self.counters):
            self.io_lock.acquire()
            sys.stdout.write('Filling job queues. \ \r')
            sys.stdout.flush()
            time.sleep(.1)
            sys.stdout.write('Filling job queues. | \r')
            sys.stdout.flush()
            time.sleep(.1)
            sys.stdout.write('Filling job queues. / \r')
            sys.stdout.flush()
            time.sleep(.1)
            sys.stdout.write('Filling job queues. - \r')
            sys.stdout.flush()
            time.sleep(.1)
            sys.stdout.flush()
            self.io_lock.release()
        while any(not c.complete() for c in self.counters):
            time.sleep(.1)
            for counter in self.counters:
                self.progress(counter)
            self.check_msgs()
            write('\r')
        write('\n')

    def progress(self, counter):
        cnt = counter.value()
        ceil = counter.ceil()
        prct = cnt * 1.0 / ceil * 100
        write('[%s: %07d of %07d %03.02f%%]\t' % (counter.name, cnt, counter.ceil(), prct))

    def check_msgs(self):
        rv = True
        if not self.msg_qu.empty():
            msg = self.msg_qu.get()
            if not msg:
                rv = False
            else:
                sys.stdout.write('<MSG: %s>' % msg)
            self.msg_qu.task_done()
        return rv


class Validator(object):

    def valid(self, obj):
        pass

class FileValidator(Validator):

    def valid(self, fname):
        return os.path.isfile(fname)

def write(msg):
    with LOCK:
        sys.stdout.write(msg)
        sys.stdout.flush()

if __name__ == '__main__':
    pdfs = []
    args = ParserFactory().new_parser().parse()
    num_procs = multiprocessing.cpu_count()/2 - 1
    #num_procs = multiprocessing.cpu_count() - 2
    mgr = multiprocessing.Manager()

    if os.path.isdir(args.pdf_in):
        dir_name = os.path.join(args.pdf_in, '*')
        pdfs = glob.glob(dir_name)
        print num_procs, 'processes analyzing', len(pdfs), 'samples in directory:', dir_name
    elif os.path.exists(args.pdf_in):
        pdfs.append(args.pdf_in)
        print num_procs, 'processes analyzing file:', args.pdf_in
    else:
        print 'Unable to find PDF file/directory:', args.pdf_in
        sys.exit(1)
    if not len(pdfs) > 0:
        print 'Empty sample set'
        sys.exit(0)

    io_lock = multiprocessing.Lock()
    jobs = multiprocessing.JoinableQueue()
    results = multiprocessing.JoinableQueue()
    msgs = multiprocessing.JoinableQueue()
    job_validator = FileValidator()
    job_counter = Counter(len(pdfs), 'Hashed')
    result_counter = Counter(len(pdfs), 'Stored')
    counters = [job_counter, result_counter]

    hashers = [ Hasher(jobs, results, job_counter, args.out) for cnt in range(num_procs) ]
    stasher = Stasher(results, args.out, result_counter)
    jobber = Jobber(pdfs, jobs, job_validator, counters, num_procs)
    progress = ProgressBar(counters, LOCK, msgs)

    write("Starting processes...\n")
    jobber.start()
    stasher.start()
    for hasher in hashers:
        hasher.start()
    progress.start()

    jobs.join()
    results.join()

    time.sleep(1)
    results.put(None)

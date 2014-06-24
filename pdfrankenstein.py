import io
import os
import sys
import glob
import time
import getopt
import hashlib
import traceback
import multiprocessing

from  peepdf.PDFCore import PDFParser

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
    '''
    def __init__(self, qin, qout, counter, io_lock):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.qout = qout
        self.counter = counter
        self.io_lock = io_lock

    def run(self):
        while True:
            pdf = self.qin.get()
            if not pdf:
                break
            pdf_name = pdf.rstrip(os.path.sep).rpartition(os.path.sep)[2]
            rv, parsed_pdf = self.parse_pdf(pdf)
	    if not rv:
                self.io_lock.acquire()
                sys.stderr.write("\npeepdf error with file: %s\n%s\n" % (pdf_name, parsed_pdf))
                self.io_lock.release()
                js = ''
                t_hash = ''
                t_str = parsed_pdf
            else:
                t_hash, t_str = self.get_tree_hash(parsed_pdf)
                js = self.get_js(parsed_pdf)
                swf = self.get_swf(parsed_pdf)
            self.qout.put( (pdf_name, t_hash, t_str, js) )
            self.counter.inc()

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
    def __init__(self, qin, storage, counter, io_lock):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.storage = StorageFactory().new_storage(storage)
        self.counter = counter
	self.io_lock = io_lock

    def run(self):
        self.storage.open()
        while True:
            t_hash = self.qin.get()
            if not t_hash:
                break
            self.storage.store(t_hash)
            self.counter.inc()
        self.storage.close()


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
    cols = ( 'pdfmd5', 'treemd5', 'tree', 'javascript', 'swf' )
    primary = 'pdfmd5'
    
    def __init__(self):
        self.db = self.DBGateway()

    def open(self):
        self.db.create_table(self.table, cols=[ ' '.join([col, 'TEXT']) for col in self.cols], primary=self.primary)

    def store(self, data_list):
        self.db.insert(self.table, cols=self.cols, vals=data_list)
    
    def close(self):
        self.db.disconnect()

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

    def __init__(self):
        self.counter = multiprocessing.RawValue('i', 0)
        self.lock = multiprocessing.Lock()

    def inc(self):
        with self.lock:
            self.counter.value += 1

    def value(self):
        with self.lock:
            return self.counter.value


class ProgressBar(object):

    def __init__(self, counter, max_cnt, io_lock):
        self.counter = counter
        self.max_cnt = max_cnt
        self.io_lock = io_lock

    def display(self):
        cnt = self.counter.value()

        while cnt < self.max_cnt:
            progress = cnt * 1.0 / self.max_cnt * 100
            self.io_lock.acquire()
            sys.stdout.write('Approx progress: %d of %d\t%.2f%%\r' % (cnt, self.max_cnt, progress))
            sys.stdout.flush()
            self.io_lock.release()
            cnt = self.counter.value()

        progress = cnt * 1.0 / self.max_cnt * 100
        self.io_lock.acquire()
        sys.stdout.write('Approx progress: %d of %d\t%.2f%%\n' % (cnt, self.max_cnt, progress))
        self.io_lock.release()

if __name__ == '__main__':
    pdfs = []
    args = ParserFactory().new_parser().parse()

    io_lock = multiprocessing.Lock()
    num_procs = multiprocessing.cpu_count()/2 - 1
    jobs = multiprocessing.Queue()
    results = multiprocessing.Queue()

    job_counter = Counter()
    result_counter = Counter()

    hashers = [ Hasher(jobs, results, job_counter, io_lock) for cnt in xrange(num_procs) ]
    stasher = Stasher(results, args.out, result_counter, io_lock)

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

    for hasher in hashers:
        hasher.start()
    stasher.start()

    true_cnt = 0
    for pdf in pdfs:
        if os.path.isfile(pdf):
	   jobs.put(pdf)
           true_cnt += 1
    for proc in xrange(num_procs):
        jobs.put(None)
    print '%d samples added to job queue' % true_cnt

    progress = ProgressBar(job_counter, true_cnt, io_lock)
    progress.display()

    print 'Collecting hashes...'
    for hasher in hashers:
        hasher.join(0.25)
    print 'Complete'

    results.put(None) 

    progress = ProgressBar(result_counter, true_cnt, io_lock)
    progress.display()

    print 'Collecting output...'
    stasher.join()
    print 'Complete'

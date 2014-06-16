import io
import os
import md5
import sys
import glob
import time
import argparse
import multiprocessing

from  peepdf.PDFCore import PDFParser


class ArgParser(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('pdf_in', help="PDF input for analysis")
        self.parser.add_argument('-o', '--out', default='t-hash-'+time.strftime("%Y-%m-%d_%H-%M-%S")+'.txt', help="Analysis output filename. Default to timestamped file in CWD")
        self.parser.add_argument('-d', '--debug', action='store_true', default=False, help="Print debugging messages")
        self.parser.add_argument('-v', '--verbose', action='store_true', default=False, help="Spam the terminal")

    def parse(self):
        try:
            args = self.parser.parse_args()
        except Exception:
            self.parser.exit(status=0, message='Usage: pdfrankenstein.py <input pdf> [-o] [-d] [-v]\n')
        else:
            return args


class Hasher(multiprocessing.Process):

    def __init__(self, qin, qout, counter):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.qout = qout
        self.counter = counter

    def run(self):
        while True:
            pdf = self.qin.get()
            if not pdf:
                break
            parsed_pdf = self.parse_pdf(pdf)
            t_hash, t_str = self.get_tree_hash(parsed_pdf)
            js = self.get_js(parsed_pdf)
            pdf_name = pdf.rstrip(os.path.sep).rpartition(os.path.sep)[2]
            self.qout.put( (pdf_name, t_hash, t_str, js) )
            self.counter.inc()

    def parse_pdf(self, pdf):
        retval, pdffile = PDFParser().parse(pdf, forceMode=True, manualAnalysis=True)
        return pdffile

    def get_js(self, pdf):
        js = ''
        for version in range(pdf.getUpdates()+1):
            containingJS = pdf.body[version].getContainingJS()
            if len(containingJS) > 0:
                for obj_id in containingJS:
                    js += self.do_js_code(obj_id, pdf)
        return js

    def get_tree_hash(self, pdf):
        tree_string = self.do_tree(pdf)
        tree_hash = md5.new(tree_string).hexdigest()
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

    def __init__(self, qin, storage, counter):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.storage = StorageFactory().get_storage(storage)
        self.counter = counter

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

    def get_storage(self, typ):
        if typ == 'stdout':
            return StdoutStorage()
        if typ == 'db':
            return DbStorage()
        else:
            return FileStorage()

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
    cols = ( 'pdfmd5', 'treemd5', 'tree', 'javascript' )
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
    def __init__(self):
        pass

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

    def __init__(self, counter, max_cnt):
        self.counter = counter
        self.max_cnt = max_cnt

    def display(self):
        cnt = self.counter.value()
        while cnt < self.max_cnt:
            progress = cnt * 1.0 / self.max_cnt * 100
            sys.stdout.write('Approx progress: %.2f%%\r' % progress)
            sys.stdout.flush()
            cnt = self.counter.value()
        progress = cnt * 1.0 / self.max_cnt * 100
        sys.stdout.write('Approx progress: %.2f%% ' % progress)

if __name__ == '__main__':
    pdfs = []
    args = ArgParser().parse()

    num_procs = multiprocessing.cpu_count() - 1
    jobs = multiprocessing.Queue()
    results = multiprocessing.Queue()

    job_counter = Counter()
    result_counter = Counter()

    hashers = [ Hasher(jobs, results, job_counter) for cnt in xrange(num_procs) ]
    stasher = Stasher(results, args.out, result_counter)


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

    for hasher in hashers:
        hasher.start()
    stasher.start()
    
    for pdf in pdfs:
        jobs.put(pdf)
    for proc in xrange(num_procs):
        jobs.put(None)
    print 'Samples added to job queue'

    progress = ProgressBar(job_counter, len(pdfs))
    progress.display()

    for hasher in hashers:
        hasher.join()
    print 'Hashing complete'

    results.put(None) 

    progress = ProgressBar(result_counter, len(pdfs))
    progress.display()

    stasher.join()
    print 'Output complete'

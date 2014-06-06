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
        self.parser.add_argument('-o', '--out', default='t-hash-'+time.strftime("%Y-%m-%d_%H-%M-%S")+'.txt', help="Analysis output filename. Default to STDOUT")
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
            t_hash = self.get_tree_hash(pdf)
            pdf_name = pdf.rstrip(os.path.sep).rpartition(os.path.sep)[2]
            self.qout.put( (pdf_name, t_hash) )
            self.counter.inc()

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

    def get_tree_string(self, pdf):
        retval, pdf = PDFParser().parse(pdf, forceMode=True, manualAnalysis=True)
        return self.do_tree(pdf)

    def get_tree_hash(self, pdf):
        tree_string = self.get_tree_string(pdf)
        tree_hash = md5.new(tree_string).hexdigest()
        return tree_hash


class Stasher(multiprocessing.Process):

    def __init__(self, qin, filename, counter):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.fname = filename
        self.counter = counter

    def run(self):
        try:
            fout = io.open(self.fname, 'wb')
        except IOError as err:
            print repr(err)
        else:
            while True:
                t_hash = self.qin.get()
                if not t_hash:
                    break
                fout.write(' '.join(t_hash) + '\n')
                self.counter.inc()
            fout.close()


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
            sys.stdout.write('Approx progress: %.2f\r' % progress)
            sys.stdout.flush()
            cnt = self.counter.value()
        progress = cnt * 1.0 / self.max_cnt * 100
        sys.stdout.write('Approx progress: %.2f ' % progress)

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

    print num_procs, 'processes analyzing', 

    if os.path.isdir(args.pdf_in):
        dir_name = os.path.join(args.pdf_in, '*')
        pdfs = glob.glob(dir_name)
        print len(pdfs), 'samples in directory:', dir_name
    elif os.path.exists(args.pdf_in):
        print 'Processing file:', args.pdf_in
        pdfs.append(args.pdf_in)
    else:
        print 'Unable to find PDF file/directory:', pdf_in
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

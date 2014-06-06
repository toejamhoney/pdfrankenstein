#!/usr/bin/env python
import os
import md5
import sys
from glob import glob
from multiprocessing import Process, Queue
from subprocess import Popen,PIPE

cmd_filename = 'tree_cmds.txt'
try:
    cmd_file = open(cmd_filename, 'r')
except IOError:
    cmd_file = open(cmd_filename, 'w')
    cmd_file.write('tree')
else:
    cmds = cmd_file.read().rstrip()
    if cmds != 'tree':
        print 'Tree command not found. Is this correct?'
        print cmd
finally:
    cmd_file.close()

dirname = os.path.join(sys.argv[1], '*')

pdf_cnt = 0
pdfs = glob(dirname)
pdf_total = len(pdfs)
print pdf_total,'items found'

with open(sys.argv[1].rstrip(os.path.sep).rpartition(os.path.sep)[2] + '-tree-hashes.txt', 'w') as fout:
    for pdf in pdfs:
        progress = (pdf_cnt * 1.0) / pdf_total * 100
        sys.stdout.write('Analyzing %d. %.2f%% files done\r' % (pdf_cnt, progress))
        sys.stdout.flush()

        cmd = ['python', 'peepdf.py', '-f', pdf, '-s', cmd_filename]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        stripped = stdout.rstrip('\x1b[0m')
        stripped = stripped.rstrip()

        tree_hash = md5.new(stripped).hexdigest()
        fout.write(' '.join([pdf.rpartition(os.path.sep)[2], tree_hash]) + '\n')
        pdf_cnt += 1

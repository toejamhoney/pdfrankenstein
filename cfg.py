import os
import sys
from ConfigParser import SafeConfigParser


DEFAULT_CFG = 'frankenstein.cfg'


class Config(object):

    def __init__(self, path=''):
        cfg_file = os.path.join(path, DEFAULT_CFG)
        self.parser = SafeConfigParser()
        if not self.parser.read(cfg_file):
            print 'No configuration file found:', cfg_file
            self.new_cfg()

    def new_cfg(self):
        self.parser.add_section('general')
        self.parser.set('general', 'output', 'sqlite3 #default')
        self.parser.set('general', '#output', 'stdout')
        self.parser.add_section('database')
        self.parser.set('database', 'path', os.getcwd())
        self.parser.set('database', 'user', 'frankenstein')
        self.parser.set('database', 'pw', 'PuttinOnTheRitz')
        self.parser.set('database', 'db', 'frankenstein.sqlite')
        with open(DEFAULT_CFG, 'w') as new_cfg:
            print 'Creating new config file in CWD:', DEFAULT_CFG
            print 'Please double check the default values before running again:'
            print self
            self.parser.write(new_cfg)
        sys.exit(0)

    def setting(self, section='', option=''):
        if section and self.parser.has_option(section, option):
            return self.parser.get(section, option)
        else:
            for sect in self.parser.sections():
                if self.parser.has_option(sect, option):
                    return self.parser.get(sect, option)
            

    def __str__(self):
        rv = ''
        for sect in self.parser.sections():
            rv += 'Section: %s\n' % sect
            for opt in self.parser.options(sect):
                rv += '\t%s\t=\t%s\n' % (opt, self.parser.get(sect, opt))
        return rv


if __name__ == '__main__':
    cfg = Config()

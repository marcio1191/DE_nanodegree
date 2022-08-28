import os
import configparser

config = configparser.ConfigParser()
config.read_file(open(f'{os.path.dirname(__file__)}/config.cfg'))
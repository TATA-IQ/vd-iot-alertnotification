"""cache code"""
from src.config_parser import Config
from caching.rediscaching import Caching

path="config/config.yaml"
configdata=Config.yamlconfig(path)[0]
# print(configdata)
api=configdata["apis"]
# print(api)
cs = Caching(api)
cs.persistData()
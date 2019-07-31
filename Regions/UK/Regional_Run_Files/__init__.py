# https://stackoverflow.com/questions/1057431/how-to-load-all-modules-in-a-folder
# logging.info(f'Invoking __init__.py for {__name__}')
# https://realpython.com/python-modules-packages/
__all__ = ['data_analysis','data_matching','data_processing','db_calls','org_suffixes','setup']
from . import *
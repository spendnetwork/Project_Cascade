import argparse
from pathlib import Path
import pandas as pd
from Config_Files import config_dirs

def get_input_args():
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    function_map = {
        'convert_to_training' : 'convert_to_training_func',
        'add_data_to_table' : 'add_data_to_table_func'
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('--priv_raw_name', default='private_data.csv', type=str,
                        help='Set raw private/source datafile name')
    parser.add_argument('--pub_raw_name', default='public_data.csv', type=str, help='Set raw public datafile name')
    parser.add_argument('--priv_adj_name', default='priv_data_adj.csv', type=str,
                        help='Set cleaned private/source datafile name')
    parser.add_argument('--pub_adj_name', default='pub_data_adj.csv', type=str, help='Set cleaned public datafile name')
    parser.add_argument('--recycle', action='store_true', help='Recycle the manual training data')
    parser.add_argument('--training', action='store_false', help='Modify/contribute to the training data')
    parser.add_argument('--config_review', action='store_true', help='Manually review/choose best config file results')
    parser.add_argument('--terminal_matching', action='store_true', help='Perform manual matching in terminal')
    parser.add_argument('--upload', choice='FUNCTION_MAP.keys()')
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    # main()
    in_args = get_input_args()
    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None
    # Define config file variables and related arguments
    config_path = Path('./Config_Files')
    config_dirs = config_dirs.dirs["dirs"]

    # Ignores config_dirs - convention is <num>_config.py
    pyfiles = "*_config.py"
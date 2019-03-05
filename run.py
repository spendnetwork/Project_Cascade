import argparse
from pathlib import Path
import pandas as pd
from Config_Files import config_dirs
import convert_training
import calls_to_db
import pdb


def get_input_args():
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    function_map = {
        'convert_to_training' : convert_training.convert_to_training,
        'add_data_to_table' : calls_to_db.add_data_to_table
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
    parser.add_argument('--convert_training', action='store_true', help='Convert confirmed matches to training file for recycle phase')
    parser.add_argument('--upload_to_db', action='store_true' , help='Add confirmed matches to database')
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

    if in_args.convert_training:
        # Ensure not in recycle mode for training file to be converted
        assert not in_args.recycle, "Failed as convert flag to be used for name_only. Run without --recycle flag."

        man_matched = pd.read_csv(
            config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
            usecols=['Manual_Match', 'priv_name_adj', 'priv_address', 'pub_name_adj', 'pub_address'])

        convert_training(config_dirs, man_matched)

    if in_args.upload_to_db:
        if not in_args.recycle:
        calls_to_db.add_data_to_table("spaziodati.confirmed_nameonly_matches")
        if in_args.recycle:
        calls_to_db.add_data_to_table("spaziodati.confirmed_nameaddress_matches")
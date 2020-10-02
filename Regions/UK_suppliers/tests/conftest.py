from shutil import copyfile
import pytest
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, DateTime, func
import directories
from Regions.UK_suppliers.Regional_Run_Files import setup
import runfile
import pdb
from pathlib import Path
import sys


# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


@pytest.fixture(scope='session', autouse=True)
def createTempProjectDirectory(tmpdir_factory):
    ''' Create a temporary root directory from which the other project folders will be creating for testing '''
    temprootdir = tmpdir_factory.mktemp('tmproot')
    return temprootdir


@pytest.fixture(scope='session', autouse=True)
def adjust_default_args(createTempProjectDirectory):
    ''' Create arg_parser from runfile but also add additional arguments necessary to locate the test files '''
    args, parser = runfile.getInputArgs(createTempProjectDirectory,[])
    parser.add_argument('--src', default='src_data_raw_test.csv', type=str)
    parser.add_argument('--reg', default='reg_data_raw_test.csv', type=str)
    parser.add_argument('--src_adj', default='src_data_adj_test.csv', type=str)
    parser.add_argument('--reg_adj', default='reg_data_adj_test.csv', type=str)
    parser.add_argument('--assigned_file', default='Outputs/Name_Only/Deduped_Data/Name_Only_matched_clust_assigned.csv', type=str)

    # Empty list as param below to avoid i.e. 'test_setup.py' being passed as an argument
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580

    args = parser.parse_args([])
    return args


@pytest.fixture(scope='session')
def settings(adjust_default_args, createTempProjectDirectory):

    import settings
    # settings = runfile.createSettingsObj(createTempProjectDirectory, adjust_default_args, settings)
    in_args = adjust_default_args

    if in_args.region == 'Italy':
        settings = settings.Italy_Settings

    if in_args.region == 'UK':
        settings = settings.UK_Settings

    if in_args.region == 'UK_entities':
        settings = settings.UK_entities

    if in_args.region == 'CQC':
        settings = settings.CQC_settings

    if in_args.region == 'UK_suppliers':
        settings = settings.UK_suppliers

    settings.in_args = in_args
    settings.region_dir = os.path.join(createTempProjectDirectory, 'Regions', in_args.region)

    # Define config file variables and attach to settings object
    settings.config_path = Path(os.path.join(settings.region_dir, 'Config_Files'))

    # establish filepath to current test directory
    settings.testdir = os.path.dirname(os.path.abspath(__file__))

    return settings


@pytest.fixture(scope='session', autouse=True)
def transfer_data_files(settings, createTempProjectDirectory):

    tmp_root = createTempProjectDirectory
    testdir = settings.testdir
    setup.Setup(settings).setupRawDirs()
    setup.Setup(settings).SetupDirs()
    settings.tmp_root = tmp_root

    print("\n\nTemporary testing directories constructed at {}".format(str(tmp_root)))

    copyfile(str(testdir) + '/test_data/src_data_raw_test.csv',
             os.path.join(settings.directories['raw_dir'].format(settings.region_dir),'src_data_raw_test.csv'))

    copyfile(str(testdir) + '/test_data/reg_data_raw_test.csv',
             os.path.join(settings.directories['raw_dir'].format(settings.region_dir), 'reg_data_raw_test.csv'))
    #
    # copyfile(str(testdir) + '/test_data/src_data_adj_test.csv',
    #          os.path.join(settings.directories['adj_dir'].format(settings.region_dir), 'src_data_adj_test.csv'))
    #
    # copyfile(str(testdir) + '/test_data/reg_data_adj_test.csv',
    #          os.path.join(settings.directories['adj_dir'].format(settings.region_dir), 'reg_data_adj_test.csv'))

    copyfile(str(testdir) + '/test_data/cluster_training.json',
             os.path.join(settings.directories['proc_type_train_clust_dir'].format(settings.region_dir, settings.proc_type), 'clustering_training.json'))

    copyfile(str(testdir) + '/test_data/matching_training.json',
             os.path.join(settings.directories['proc_type_train_match_dir'].format(settings.region_dir, settings.proc_type),
                          'matching_training.json'))

    copyfile(str(testdir) + '/test_data/test_clustered_assigned.csv',
             settings.directories['assigned_output_file'].format(settings.region_dir, settings.proc_type))

    copyfile(str(testdir) + '/test_data/testconfig.py', os.path.join(settings.region_dir, 'Config_Files', '1_config.py'))

    return tmp_root


@pytest.fixture()
def connection():
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn # CHECK THIS - SHOULD IT BE RETURN CUR???

@pytest.fixture()
def create_reg_table(postgresql_db):
    # Combination of SQLAlchemy to define classes in python which get converted to an sql schema, and pytest_pgsql to
    # make use of postgresql_db to create and delete tables when testing
    class orgs_ocds_test(declarative_base()):
        __tablename__ = 'orgs_ocds_test'

        scheme = Column(String)
        id = Column(String)
        uri = Column(String)
        legalname = Column(String)
        source = Column(String)
        created_at = Column(DateTime(timezone=True), server_default=func.now()) # https://stackoverflow.com/questions/13370317/sqlalchemy-default-datetime
        orgs_ocds_scheme_id_idx = Column(Integer, primary_key=True)

    postgresql_db.create_table(orgs_ocds_test)

    postgresql_db.load_csv('test_data/orgs_ocds_test_data.csv', orgs_ocds_test)
    orgs_ocds_test = postgresql_db.get_table('orgs_ocds_test')

    return orgs_ocds_test

@pytest.fixture()
def create_src_table(postgresql_db):
    # Combination of SQLAlchemy to define classes in python which get converted to an sql schema, and pytest_pgsql to
    # make use of postgresql_db to create and delete tables when testing
    class ocds_tenders_test(declarative_base()):
        __tablename__ = 'ocds_tenders_test'
        src_name = Column(String)
        src_tag = Column(String)
        src_address_locality = Column(String)
        src_address_postalcode = Column(String)
        src_address_countryname = Column(String)
        src_address_streetaddress = Column(String)
        source = Column(String)
        id_idx = Column(Integer, primary_key=True)

    postgresql_db.create_table(ocds_tenders_test)

    postgresql_db.load_csv('test_data/src_data_raw_test.csv', ocds_tenders_test)
    ocds_tenders_test = postgresql_db.get_table('ocds_tenders_test')

    return ocds_tenders_test

@pytest.fixture()
def create_upload_table(postgresql_db):
    # Combination of SQLAlchemy to define classes in python which get converted to an sql schema, and pytest_pgsql to
    # make use of postgresql_db to create and delete tables when testing
    class upload_table_test(declarative_base()):
        __tablename__ = 'upload_table_test'

        src_name = Column(String)
        reg_name = Column(String)
        leven_dist_n = Column(Integer)
        manual_match_n = Column(String)
        src_address_adj = Column(String)
        reg_address_adj = Column(String)
        manual_match_na = Column(String)
        leven_dist_na = Column(Integer)
        reg_id = Column(String)
        src_tag = Column(String)
        src_id = Column(String)
        match_source = Column(String)
        created_at = Column(String)
        reg_scheme = Column(String)
        id = Column(Integer, primary_key=True)
        created_at = Column(DateTime(timezone=True), server_default=func.now())
        match_by = Column(String)

    postgresql_db.create_table(upload_table_test)

    upload_table_test = postgresql_db.get_table('upload_table_test')

    return upload_table_test


@pytest.fixture()
def test_create_reg_table(postgresql_db):
    # Combination of SQLAlchemy to define classes in python which get converted to an sql schema, and pytest_pgsql to
    # make use of postgresql_db to create and delete tables when testing
    class orgs_ocds_test(declarative_base()):
        __tablename__ = 'orgs_ocds_test'

        scheme = Column(String)
        id = Column(String)
        uri = Column(String)
        legalname = Column(String)
        source = Column(String)
        created_at = Column(DateTime(timezone=True), server_default=func.now()) # https://stackoverflow.com/questions/13370317/sqlalchemy-default-datetime
        orgs_ocds_scheme_id_idx = Column(Integer, primary_key=True)


    postgresql_db.create_table(orgs_ocds_test)

    postgresql_db.load_csv('test_data/orgs_ocds_test_data.csv', orgs_ocds_test)

    orgs_ocds_test = postgresql_db.get_table('orgs_ocds_test')

    return postgresql_db, orgs_ocds_test


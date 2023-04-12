import os
import re
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import shutil
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from utilities import add_objpipe_to_path
from utilities import prepare_files
from config_parser import ConfigParser
import logging

from s3path import PureS3Path

add_objpipe_to_path('/home/ubuntu/ObjPipe/objPipe')
from objPipe.subject import HcpSubject

CONFIG_LOC = '/home/ubuntu/test/ObjPipe/src/hcp_config.ini'


def does_exist(path):
    """Check if a path exists on an s3 bucket.
    :param path: the path to check
    :return: True if the path exists, False otherwise
    """
    command = f'aws s3 ls {path}'
    try:
        subprocess.check_output(command, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


def print_banner(banner_text):
    print('*' * 80)
    # put the banner text in middle with **** filling in both sides
    print(f'********{banner_text.center(64)}********')
    print('*' * 80)


class HcpPipeline:
    def __init__(self,
                 caselist_file: str,
                 group_name: str,
                 hcp_data_root: str,
                 s3_bucket_hcp_root: str,
                 bids_study_root: str,
                 config_loc: str,
                 log_loc: str,
                 temp_log_loc: str,
                 start_index: int,
                 end_index: int,
                 dry_run: bool):
        """ Initializes the HCP pipeline
        Parameters
        ----------
        caselist_file: str
            the path to the caselist file
        group_name: str
            the name of the group to process
        hcp_data_root: str
            the root directory where the HCP data will be stored
        s3_bucket_hcp_root: str
            the root directory of the HCP data on the S3 bucket
        bids_study_root: str
            the root directory of the BIDS study
        config_loc: str
            the path to the config file
        log_loc: str
            the path to the log file
        temp_log_loc: str
            the path to the temporary log file
        dry_run: bool
            whether to run the pipeline in dry run mode, the default is True,
            which means torun the pipeline you need to add the -r flag to the
            command line arguments
        """
        print('initializing HCP pipeline')
        self.config_loc = config_loc

        self._set_class_fields_from_config(self.config_loc)

        # Override the class attributes from config file if command line arguments
        # are provided
        if caselist_file is not None:
            self.caselist_file = PureS3Path(caselist_file)
        if group_name is not None:
            self.group_name = group_name
        if hcp_data_root is not None:
            self.hcp_data_root = PureS3Path(hcp_data_root)
        if s3_bucket_hcp_root is not None:
            self.s3_bucket_hcp_root = PureS3Path.from_uri(s3_bucket_hcp_root)
        if bids_study_root is not None:
            self.bids_study_root = PureS3Path(bids_study_root)
        if log_loc is not None:
            self.log_loc = Path(log_loc)
        if temp_log_loc is not None:
            self.temp_log_loc = Path(temp_log_loc)
        if dry_run is not None:
            self.dry_run = dry_run
        if start_index is not None:
            self.start_index = start_index
        if end_index is not None:
            self.end_index = end_index

        # print class attributes
        self._print_class_attributes()

        self.caselist = self._get_caselist(self.start_index, self.end_index)
        self.subjects = self._get_subjects()


        print('done initializing HCP pipeline')

    def _set_class_fields_from_config(self, config_loc: str):
        """Sets the class attributes from the config file
        Parameters
        ----------
        config_loc: str
            the path to the config file
        """
        self.config = ConfigParser(config_loc)
        self.caselist_file = PureS3Path(self.config.get('caselist_file'))
        self.group_name = self.config.get('group_name')
        self.hcp_data_root = PureS3Path(self.config.get('hcp_data_root'))
        self.s3_bucket_hcp_root = PureS3Path.from_uri(self.config.get('s3_bucket_hcp_root'))
        self.bids_study_root = PureS3Path(self.config.get('bids_study_root'))
        self.dry_run = self.config.get('dry_run')
        self.start_index = int(self.config.get('start_index'))
        self.end_index = int(self.config.get('end_index'))
        self.log_loc = Path(self.config.get('log_loc'))
        self.temp_log_loc = Path(self.config.get('temp_log_loc'))

    def _print_class_attributes(self):
        """Prints the class attributes"""
        print_banner('HCP Pipline Class Attributes')
        print('caselist_file: ', self.caselist_file)
        print('group_name: ', self.group_name)
        print('hcp_data_root: ', self.hcp_data_root)
        print('s3_bucket_hcp_root: ', self.s3_bucket_hcp_root)
        print('bids_study_root: ', self.bids_study_root)
        print('config_loc: ', self.config_loc)
        print('log_loc: ', self.log_loc)
        print('temp_log_loc: ', self.temp_log_loc)
        print('start_index: ', self.start_index)
        print('end_index: ', self.end_index)
        print('dry_run: ', self.dry_run)
        print('*' * 80)

    def _get_caselist(self, start_index=1, end_index=None):
        """
        creates a list from the caselist file, ignoring any lines that start with
        the comment character '#', starting at the start_index and ending at the
        end_index,[start, end] both inclusive,  if end_index is None then it
        will go to the end of the list. Uses 1 based indexing, so the first line
        in the caselist file is line 1, not line 0.
        Parameters
        ----------
        start_index: int
            the index of the first line to read from the caselist file
        end_index: int
            the index of the last line to read from the caselist file
        Returns
        -------
        caselist: list
            a list of the subjects to process
        """
        print('getting caselist')
        caselist = []
        with open(self.caselist_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    caselist.append(line)
        if end_index is None:
            end_index = len(caselist)
        caselist = caselist[start_index - 1:end_index]
        print(f'caselist: {caselist}')
        return caselist

    def _get_subjects(self):
        """
        creates a list of subjects to process from the caselist file, ignoring
        any lines that start with adding the _V1_MR suffix to the subject names
        if it is not already there
        """
        print('getting subjects')
        subjects = []
        for subject in self.caselist:
            # append _V1_MR to subject names in caselist
            if not re.search(r'_V\d_MR', subject):
                subject = subject + '_V1_MR'
            subject_path = self.s3_bucket_hcp_root / self.group_name / subject
            print(f'subject_path: {subject_path.as_uri()}')
            if does_exist(subject_path):
                subjects.append(subject)
        print(f'subjects: {subjects}')
        return subjects

    def _sync_subject_data(self, subject):
        """ syncs the subject data from the S3 bucket to the local machine
        Parameters
        ----------
        subject: str
            the name of the subject to sync
        """
        dry_run = self.dry_run
        subject_path = self.s3_bucket_hcp_root / self.group_name / subject
        print(f'subject_path: {subject_path.as_uri()}')
        if does_exist(subject_path.as_uri()):
            subject_name = subject_path.parts[-1]
            save_path = self.hcp_data_root / self.group_name / subject_name
            if dry_run:
                sync_command = f'aws s3 sync {subject_path.as_uri()} {save_path} --dryrun'
            else:
                sync_command = f'aws s3 sync {subject_path.as_uri()} {save_path}'
            print(sync_command)
            subprocess.call(sync_command, shell=True)

    def _run_hcp_subject(self, subject):
        """ runs the HCP subject pipeline
        Parameters
        ----------
        subject: str
            the name of the subject to run
        """
        print(f'running HCP subject: {subject}')
        nifti_path = self.hcp_data_root / self.group_name / subject / 'unprocessed' / 'Diffusion'
        subject_name = re.sub(r'_V\d_MR', '', subject)
        session_name = re.search(r'V\d', subject).group(0).replace('V', '')
        hcpSubject = HcpSubject(
            str(nifti_path),
            session_name=session_name,
            subject_name=subject_name,
            bids_study_root=self.bids_study_root,
            config_loc=self.config_loc)
        hcpSubject.run_pipeline(unring=False)

    def _upload_subject_data(self, subject):
        """ uploads the subject data from the local machine to the S3 bucket
        Parameters
        ----------
        subject: str
            the name of the subject to upload
        """
        dry_run = self.dry_run
        subject_path = self.hcp_data_root / self.group_name / subject
        bucket_path = self.s3_bucket_hcp_root / self.group_name / subject
        if dry_run:
            sync_command = f'aws s3 sync {subject_path} {bucket_path.as_uri()} --dryrun'
        else:
            sync_command = f'aws s3 sync {subject_path} {bucket_path.as_uri()}'
        print(sync_command)
        subprocess.call(sync_command, shell=True)

    def _verify_subject_data(self, subject):
        """ verifies that the subject data has been uploaded to the S3 bucket
        Parameters
        ----------
        subject: str
            the name of the subject to verify
        Returns
        -------
        bool
            True if the subject data exists in the S3 bucket, False otherwise
        """
        subject_path = self.s3_bucket_hcp_root / self.group_name / subject / 'derivatives' / 'dwipreproc' / 'Diffusion'
        if does_exist(subject_path.as_uri()):
            return True
        else:
            return False

    def _delete_subject_data(self, subject):
        """ deletes the subject data from the local machine
        Parameters
        ----------
        subject: str
            the name of the subject to delete
        """
        subject_path = self.hcp_data_root / self.group_name / subject
        print(f'deleting {subject_path}')

        if subject_path.is_absolute():
            shutil.rmtree(subject_path)

    def _log(self, message, subject):
        """ logs a message to a file and to the console
        Parameters
        ----------
        message: str
            the message to log
        subject: str
            the subject to log the message for
        """
        log_file = self.log_loc
        if not log_file.is_absolute():
            log_file = self.hcp_data_root / log_file
        print(f'log_file: {log_file}')
        if not log_file.parent.exists():
            log_file.parent.mkdir(parents=True)
        if not log_file.exists():
            log_file.touch()
        logging.basicConfig(filename=log_file, level=logging.INFO)
        logging.info(f'{datetime.now()}: {message} {subject}')

    def _get_logs_from_s3(self):
        """ gets the log messages from the S3 bucket
        Returns
        -------
        str
            the log messages from the S3 bucket
        """
        log_file = self.s3_bucket_hcp_root / self.group_name / self.log_loc.name
        # make a temp log file to store the log messages from S3 so it does not
        # overwrite the local log file that needs appended to it
        if does_exist(log_file.as_uri()):
            command = f'aws s3 cp {log_file.as_uri()} {self.temp_log_loc} --no-progress'
            print(command)
            subprocess.call(command, shell=True)
            log = self.log_loc.read_text()
            print(f'log: {log}')
        else:
            log = ''
        return log

    def _copy_logs_to_s3(self):
        """ copies the log messages to the S3 bucket"""
        dry_run = self.dry_run
        log = self._get_logs_from_s3()
        if log:
            log = log + self.log_loc.read_text()
        else:
            log = self.log_loc.read_text()
        # remove any duplicate log messages
        log = '\n'.join(set(log.split('\n')))
        self.log_loc.write_text(log)
        log_file = self.s3_bucket_hcp_root / self.group_name / self.log_loc.name
        if dry_run:
            sync_command = f'aws s3 cp {self.log_loc} {log_file.as_uri()}  --dryrun'
        else:
            sync_command = f'aws s3 cp {self.log_loc} {log_file.as_uri()} '
        print(sync_command)
        subprocess.call(sync_command, shell=True)

    def _prepare_cnn_masking_files(self, subject):
        """ Renames three files to have a uniform unique prefix for the CNN
        masking pipeline
        Parameters
        ----------
        subject: str
            the name of the subject to fix
        """
        pipeline_output = Path(self.hcp_data_root / self.group_name / subject /
                               'derivatives' / 'dwipreproc' / 'Diffusion')
        print(f'preparing CNN file format for {subject} in {pipeline_output}...')
        prefix = re.sub(r'_V\d_MR', '', subject)
        print(f'prefix: {prefix}')
        prepare_files(directory=pipeline_output, prefix=prefix)

    def run_pipeline(self):
        """ runs the pipeline for all subjects in the caselist file """
        t0 = time.perf_counter()
        for subject in self.subjects:
            print(f'Running pipeline for {subject}...')
            self._sync_subject_data(subject)
            self._run_hcp_subject(subject)
            self._prepare_cnn_masking_files(subject)
            self._upload_subject_data(subject)
            if self._verify_subject_data(subject):
                self._log('Completed', subject)
            else:
                self._log('Error', subject)

            self._delete_subject_data(subject)
        self._copy_logs_to_s3()
        t1 = time.perf_counter()
        print(f'Finished pipeline in {t1 - t0} seconds')


if __name__ == '__main__':
    # use argparse to parse command line arguments both short and long form
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--caselist_file', type=str, default=None)
    parser.add_argument('-g', '--group_name', type=str, default=None)
    parser.add_argument('-d', '--hcp_data_root', type=str, default=None)
    parser.add_argument('-s', '--s3_bucket_hcp_root', type=str, default=None)
    parser.add_argument('-b', '--bids_study_root', type=str, default=None)
    parser.add_argument('-c', '--config_loc', type=str, default=CONFIG_LOC)
    parser.add_argument('-l', '--log_loc', type=str, default=None)
    parser.add_argument('-t', '--temp_log_loc', type=str, default=None)
    parser.add_argument('-i', '--start_index', type=int, default=None)
    parser.add_argument('-e', '--end_index', type=lambda x: None if x == 'None' else int(x), default=None)
    parser.add_argument('-r', '--dry_run', action='store_false')
    args = parser.parse_args()

    # instantiate pipeline object
    hcpPipeline = HcpPipeline(
        caselist_file=args.caselist_file,
        group_name=args.group_name,
        hcp_data_root=args.hcp_data_root,
        s3_bucket_hcp_root=args.s3_bucket_hcp_root,
        bids_study_root=args.bids_study_root,
        config_loc=args.config_loc,
        log_loc=args.log_loc,
        temp_log_loc=args.temp_log_loc,
        start_index=args.start_index,
        end_index=args.end_index,
        dry_run=args.dry_run)
    # run pipeline
    hcpPipeline.run_pipeline()


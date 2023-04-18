""" Establishes the HCP pipeline for masking the DWI data from the S3 bucket
and uploading the masked data to the S3 bucket.

Steps:
1. Read <batch_size> subjects at a time from the caselist file
2. copy the subject files from the S3 bucket
        ├── s3://nda-enclave-c3371/HCP/<group_name>/<subject_id>_V1_MR/derivatives/dwipreproc/Diffusion/<subject_id>_EdEp.bval
        ├── s3://nda-enclave-c3371/HCP/<group_name>/<subject_id>_V1_MR/derivatives/dwipreproc/Diffusion/<subject_id>_EdEp.bvec
        └── s3://nda-enclave-c3371/HCP/<group_name>/<subject_id>_V1_MR/derivatives/dwipreproc/Diffusion/<subject_id>_EdEp.nii.gz
    to a temporary directory on the EC2 instance at
        ├── /data/HCP/<group_name>/<subject_id>_V1_MR/derivatives/<brainmasks>/<subject_id>_EdEp.bval
        ├── /data/HCP/<group_name>/<subject_id>_V1_MR/derivatives/<brainmasks>/<subject_id>_EdEp.bvec
        └── /data/HCP/<group_name>/<subject_id>_V1_MR/derivatives/<brainmasks>/<subject_id>_EdEp.nii.gz
3. update process list to include just the nii.gz file path to each subject in the directory where each line is a subject
4. run the masking pipeline on the subjects on the caselist
5. upload the masked data and any other relevant files to the S3 bucket for each
    subject, making a new directory for masked data in the Derivatives folder calling
    the new directory <subject_id>_EdEp_masked
6. delete the temporary directory containing the subject files once the upload is complete
7. add subjects to log as completed or failed depending on whether the masking pipeline
    was successful
8. repeat steps 1-7 until all subjects have been processed
"""

import os
import re
import sys

import time
import shutil
import argparse
import subprocess
from pathlib import Path
from config_parser import ConfigParser
import logging
from multiprocessing import Pool, cpu_count
from s3path import PureS3Path

CONFIG_LOC = '/home/ec2-user/CNN-Diffusion-MRIBrain-Segmentation/pipeline/hcp_config.ini'


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


class HcpMaskingPipeline:
    """ The HCP pipeline for masking the DWI data from the S3 bucket and
    uploading the masked data to the S3 bucket.
    """
    batches_completed = 0

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
                 batch_size: int,
                 input_text: str,
                 model_folder: str,
                 additional_files_loc: str,
                 masking_script: str,
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
        self.allowed_files = ['_EdEp.bval', '_EdEp.bvec', '_EdEp.nii.gz', '_EdEp_bse-multi_BrainMask.nii.gz', '_EdEp_bse.nii.gz']
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
            self.hcp_data_root = Path(hcp_data_root)
        if s3_bucket_hcp_root is not None:
            self.s3_bucket_hcp_root = PureS3Path.from_uri(s3_bucket_hcp_root)
        if bids_study_root is not None:
            self.bids_study_root = Path(bids_study_root)
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
        if batch_size is not None:
            self.batch_size = batch_size
        if input_text is not None:
            self.input_text = input_text
        if model_folder is not None:
            self.model_folder = model_folder
        if additional_files_loc is not None:
            self.additional_files_loc = Path(additional_files_loc)
        if masking_script is not None:
            self.masking_script = masking_script

        # print class attributes
        self._print_class_attributes()

        self.caselist = self._get_caselist(self.start_index, self.end_index)
        self.subjects = self._get_subjects()
        # set subjects_to_process based on the batch size
        self.subjects_to_process = self.subjects

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
        self.group_name = str(self.config.get('group_name'))
        self.hcp_data_root = PureS3Path(self.config.get('hcp_data_root'))
        self.s3_bucket_hcp_root = PureS3Path.from_uri(self.config.get('s3_bucket_hcp_root'))
        self.bids_study_root = PureS3Path(self.config.get('bids_study_root'))
        self.dry_run = self.config.get('dry_run')
        self.start_index = int(self.config.get('start_index'))
        self.end_index = int(self.config.get('end_index'))
        self.batch_size = int(self.config.get('batch_size'))
        self.log_loc = Path(self.config.get('log_loc'))
        self.input_text = Path(self.config.get('input_text'))
        self.model_folder = Path(self.config.get('model_folder'))
        self.temp_log_loc = Path(self.config.get('temp_log_loc'))
        self.additional_files_loc = Path(self.config.get('additional_files_loc'))
        self.masking_script = Path(self.config.get('masking_script'))

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
        print('batch_size: ', self.batch_size)
        print('input_text: ', self.input_text)
        print('model_folder: ', self.model_folder)
        print('additional_files_loc: ', self.additional_files_loc)
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

    def _copy_subject_data_from_s3(self, subject):
        """ copies the subject data from the HCP bucket to the required location
        Parameters
        ----------
        subject: str
            the name of the subject to sync
        """
        dry_run = self.dry_run
        print_banner(f'Copying Subject Data for {subject}')
        subject_path = self.s3_bucket_hcp_root / self.group_name / subject / 'derivatives' / 'dwipreproc' / 'Diffusion'
        print(f'subject_path: {subject_path.as_uri()}')
        if does_exist(subject_path.as_uri()):
            print(f'{subject_path.as_uri()} exists')
            subject_name = subject.split('_')[0]
            save_path = self.hcp_data_root / self.group_name / subject / 'derivatives' / 'harmonization'
            if not dry_run:
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*"'
            else:
                print(f'dry_run: {dry_run}')
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*" --dryrun'
            print(f'copy_command: {copy_command}')
            subprocess.call(copy_command, shell=True)

    @staticmethod
    def copy_subject_data_from_s3(subject_data):
        """ copies the subject data from the HCP bucket to the required location
        using multiprocessing

        Parameters
        ----------
        subject_data : (HcpMaskingPipeline, subject)
            a tuple containing the HcpMaskingPipeline object and the subject to
            process
        """
        hcp_pipeline, subject = subject_data
        dry_run = hcp_pipeline.dry_run
        print_banner(f'Copying Subject Data for {subject}')
        subject_path = hcp_pipeline.s3_bucket_hcp_root / hcp_pipeline.group_name / subject / 'derivatives' / 'dwipreproc' / 'Diffusion'
        print(f'subject_path: {subject_path.as_uri()}')
        if does_exist(subject_path.as_uri()):
            print(f'{subject_path.as_uri()} exists')
            subject_name = subject.split('_')[0]
            save_path = hcp_pipeline.hcp_data_root / hcp_pipeline.group_name / subject / 'derivatives' / 'harmonization'
            if not dry_run:
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*"'
            else:
                print(f'dry_run: {dry_run}')
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*" --dryrun'
            print(f'copy_command: {copy_command}')
            subprocess.call(copy_command, shell=True)

    def _create_process_list(self):
        """  update process list to include just the nii.gz file path to each
        subject in the directory where each line is a subject
        """
        print('creating process list')
        process_list = []
        root_dir = Path(self.hcp_data_root / self.group_name)
        # get all the subject directories and add the path to the process list of each .nii.gz file walking the directory
        for subject_dir in root_dir.iterdir():
            if subject_dir.is_dir():
                for file in subject_dir.rglob('*_EdEp.nii.gz'):
                    process_list.append(file)
        print(f'process_list: {process_list}')
        return process_list

    def _create_input_text(self):
        """ converts the process list to a  text file that can be used as input
        Parameters
        ----------
        Returns
        -------
        input_text: str
            the text to write to the input text file
        """
        process_list = self._create_process_list()
        # open file at input_txt location and write each subject to a new line in the file
        with open(self.input_text, 'w') as f:
            for subject in process_list:
                f.write(f'{subject.as_posix()}' + '\n')
        # check that the file was written correctly by opening it and adding contents to list and comparing to process list
        with open(self.input_text, 'r') as f:
            input_list = [line.rstrip() for line in f]
        print(f'input_list: {input_list}')

        # make all the values in the process list as strings in case they are PosixPaths
        process_list = list(map(str, process_list))

        if sorted(input_list) == sorted(process_list):
            print('input_list matches process_list')
        else:
            raise ValueError('input_list does not match process_list')

    def _upload_subject_data(self, subject):
        """ uploads the subject data from the processed directory to the S3 bucket
        Parameters
        ----------
        subject: str
            the name of the subject to upload
        """
        dry_run = self.dry_run
        print_banner(f'Uploading Subject Data for {subject}')
        subject_path = self.hcp_data_root / 'processed' / self.group_name / subject / 'derivatives' / 'harmonization'
        print(f'subject_path: {subject_path.as_uri()}')
        if does_exist(subject_path.as_uri()):
            print(f'{subject_path.as_uri()} exists')
            subject_name = subject.split('_')[0]
            save_path = self.s3_bucket_hcp_root / self.group_name / subject / 'derivatives' / 'harmonization'
            if not dry_run:
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*"'
            else:
                print(f'dry_run: {dry_run}')
                copy_command = f'aws s3 cp {subject_path.as_uri()} ' \
                               f'{save_path} --recursive --exclude "*" --include "*_EdEp*" --dryrun'
            print(f'copy_command: {copy_command}')
            subprocess.call(copy_command, shell=True)


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
        pass

    def _delete_subject_data(self, subject):
        """ deletes the subject data from the local machine
        Parameters
        ----------
        subject: str
            the name of the subject to delete
        """
        # subject_path = self.hcp_data_root / self.group_name / subject
        # print(f'deleting {subject_path}')
        #
        # if subject_path.is_absolute():
        #     shutil.rmtree(subject_path)
        pass

    def _log(self, message, subject):
        """ logs a message to a file and to the console
        Parameters
        ----------
        message: str
            the message to log
        subject: str
            the subject to log the message for
        """
        # log_file = self.log_loc
        # if not log_file.is_absolute():
        #     log_file = self.hcp_data_root / log_file
        # print(f'log_file: {log_file}')
        # if not log_file.parent.exists():
        #     log_file.parent.mkdir(parents=True)
        # if not log_file.exists():
        #     log_file.touch()
        # logging.basicConfig(filename=log_file, level=logging.INFO)
        # logging.info(f'{datetime.now()}: {message} {subject}')
        pass

    def _get_logs_from_s3(self):
        """ gets the log messages from the S3 bucket
        Returns
        -------
        str
            the log messages from the S3 bucket
        """
        # log_file = self.s3_bucket_hcp_root / self.group_name / self.log_loc.name
        # # make a temp log file to store the log messages from S3 so it does not
        # # overwrite the local log file that needs appended to it
        # if does_exist(log_file.as_uri()):
        #     command = f'aws s3 cp {log_file.as_uri()} {self.temp_log_loc} --no-progress'
        #     print(command)
        #     subprocess.call(command, shell=True)
        #     log = self.log_loc.read_text()
        #     print(f'log: {log}')
        # else:
        #     log = ''
        # return log
        pass

    def _copy_logs_to_s3(self):
        """ copies the log messages to the S3 bucket"""
        # dry_run = self.dry_run
        # log = self._get_logs_from_s3()
        # if log:
        #     log = log + self.log_loc.read_text()
        # else:
        #     log = self.log_loc.read_text()
        # # remove any duplicate log messages
        # log = '\n'.join(set(log.split('\n')))
        # self.log_loc.write_text(log)
        # log_file = self.s3_bucket_hcp_root / self.group_name / self.log_loc.name
        # if dry_run:
        #     sync_command = f'aws s3 cp {self.log_loc} {log_file.as_uri()}  --dryrun'
        # else:
        #     sync_command = f'aws s3 cp {self.log_loc} {log_file.as_uri()} '
        # print(sync_command)
        # subprocess.call(sync_command, shell=True)
        pass

    def _run_cnn_masking(self):
        """ runs the HCP subject pipeline using the input text file and model folder
        as arguments into the dwi_masking.py script
        """
        print_banner('Running Brainmasking Pipeline')
        # run the brainmasking pipeline making sure the dmri_seg conda environment is activated
        # activate dmri_seg
        conda_env = 'dmri_seg'
        env_location = '/home/ec2-user/anaconda3/envs/dmri_seg'
        activate_env = f'source {env_location}/bin/activate {conda_env}'
        print(f'activate_env: {activate_env}')
        subprocess.call(activate_env, shell=True)
        # run the brainmasking pipeline
        run_command = f'python {self.masking_script} ' \
                      f'-i {self.input_text} ' \
                      f'-f {self.model_folder}'
        print(f'run_command: {run_command}')
        subprocess.call(run_command, shell=True)

    def run_pipeline(self):
        """ runs the pipeline for all subjects in the caselist file """
        t0 = time.perf_counter()
        # for subject in self.subjects:
        # download a total number of subjects equual to the batch size and process them and then upload them and then delete them and the
        # repeat until all subjects have been processed

        while len(self.subjects_to_process) > 0:
            if len(self.subjects_to_process) < self.batch_size:
                self.batch_size = len(self.subjects_to_process)

            # get the subjects to process
            subjects_to_process = self.subjects_to_process[:self.batch_size]
            # remove the subjects from the list of subjects to process
            self.subjects_to_process = self.subjects_to_process[self.batch_size:]
            # process the subjects
            print(f'subjects_to_process: {subjects_to_process}')
            # copy subjects from S3
            with Pool(min(cpu_count(), self.batch_size)) as pool:
                pool.map(HcpMaskingPipeline.copy_subject_data_from_s3, [(self, subject) for subject in subjects_to_process])

            self._create_input_text()
            self._run_cnn_masking()

            for subject in subjects_to_process:
                self._move_subject_data_to_processed(subject)

        # clean up the subjects directories
        self._cleanup_additional_files()

            # # clean up the subjects
            # for subject in subjects_to_process:
            #     self._upload_subject_data(subject)
            #     if self._verify_subject_data(subject):
            #         self._log('Completed', subject)
            #     else:
            #         self._log('Error', subject)
            #     self._delete_subject_data(subject)

        #     self._copy_subject_data_from_s3(subject)
        # self._create_input_text()
        # self._run_brainmasking_pipeline()
        #     self._run_brainmasking_pipeline(subject)
        #     self._upload_subject_data(subject)
        #     if self._verify_subject_data(subject):
        #         self._log('Completed', subject)
        #     else:
        #         self._log('Error', subject)
        #
        #     self._delete_subject_data(subject)
        # self._copy_logs_to_s3()
        t1 = time.perf_counter()
        print(f'Finished pipeline in {t1 - t0} seconds')

    # def _get_subjects_to_process(self):
    #     """ gets the subjects to process from the caselist file
    #     Returns
    #     -------
    #     list of str
    #         the subjects to process
    #     """
    #     subjects = []
    #     with open(self.caselist_file) as f:
    #         for line in f:
    #             subjects.append(line.strip())
    #     return subjects
    #     pass

    def _move_subject_data_to_processed(self, subject):
        """ moves the subject data to the processed directory,
        Parameters
        ----------
        subject : str
            the subject to move
        """
        # get the subject directory
        subject_dir = Path(self.hcp_data_root / self.group_name / subject)

        # get the processed directory
        processed_dir = Path(self.hcp_data_root / 'processed' / self.group_name)
        if not processed_dir.exists():
            # make any parent directories that don't exist as well
            processed_dir.mkdir(parents=True, exist_ok=True)
        print(f'moving {subject_dir} to {processed_dir}')

        # move the subject directory to the processed directory
        shutil.move(str(subject_dir), str(processed_dir))

    def _cleanup_additional_files(self):
        """ cleans up any additional files that were created during the pipeline
        Walks through the processed/<group_id>/<subject>/derivatives/harmonization
         directory and moves any files that are not of the following types:
            ├── <subject>_EdEp_bse-multi_BrainMask.nii.gz
            ├── <subject>_EdEp_bse.nii.gz
            ├── <subject>_EdEp.bval
            ├── <subject>_EdEp.bvec
            └── <subject>_EdEp.nii.gz
        into the additional folders location
        """
        print_banner('Cleaning up files')
        # check to see if the self.additional_files_loc folder exists, this is where we will move any additional files to
        if not self.additional_files_loc.exists():
            self.additional_files_loc.mkdir(parents=True)

        # get the processed directory
        processed_dir = Path(self.hcp_data_root / 'processed' / self.group_name)
        # walk through each of the subjects folders in the derivatives/harmonization directory
        for subject_dir in processed_dir.iterdir():
            if subject_dir == self.additional_files_loc:
                continue
            # get the derivatives/harmonization directory
            derivatives_dir = subject_dir / 'derivatives' / 'harmonization'
            # walk through the files in the derivatives/harmonization directory
            for file in derivatives_dir.iterdir():
                # if file name is process_id.txt, delete it
                if file.name == 'process_id.txt':
                    file.unlink()
                    continue
                # check to see if the file is one of the files we want to keep
                if not str(file.as_uri()).endswith(tuple(self.allowed_files)):
                    shutil.move(str(file), str(self.additional_files_loc))





if __name__ == '__main__':
    # use argparse to parse command line arguments both short and long form
    parser = argparse.ArgumentParser()
    parser.add_argument('-cf', '--caselist_file', type=str, default=None)
    parser.add_argument('-gn', '--group_name', type=str, default=None)
    parser.add_argument('-hr', '--hcp_data_root', type=str, default=None)
    parser.add_argument('-sr', '--s3_bucket_hcp_root', type=str, default=None)
    parser.add_argument('-br', '--bids_study_root', type=str, default=None)
    parser.add_argument('-cl', '--config_loc', type=str, default=CONFIG_LOC)
    parser.add_argument('-ll', '--log_loc', type=str, default=None)
    parser.add_argument('-tl', '--temp_log_loc', type=str, default=None)
    parser.add_argument('-si', '--start_index', type=int, default=None)
    parser.add_argument('-ei', '--end_index', type=lambda x: None if x == 'None' else int(x), default=None)
    parser.add_argument('-b', '--batch_size', type=int, default=None)
    parser.add_argument('-i', '--input_text', type=str, default=None)
    parser.add_argument('-f', '--model_folder', type=str, default=None)
    parser.add_argument('-af', '--additional_files_loc', type=str, default=None)
    parser.add_argument('-ms', '--masking_script', type=str, default=None)
    parser.add_argument('-dr', '--dry_run', action='store_false')
    args = parser.parse_args()

    # instantiate pipeline object
    hcpMaskingPipeline = HcpMaskingPipeline(
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
        batch_size=args.batch_size,
        input_text=args.input_text,
        model_folder=args.model_folder,
        additional_files_loc=args.additional_files_loc,
        masking_script=args.masking_script,
        dry_run=args.dry_run)
    # run pipeline
    hcpMaskingPipeline.run_pipeline()

# CNN-Diffusion-MRIBrain-Segmentation_HCP

## Adopted CNN Masking Pipeline Instructions for processing HCP data on AWS

![PNL BWH HMS logo](docs/pnl-bwh-hms.png "PNL BWH HMS logo")

[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.3665739.svg)](https://doi.org/10.5281/zenodo.3665739) ![Python](https://img.shields.io/badge/Python-3.6-green.svg)
![Platform](https://img.shields.io/badge/Platform-linux--64%20%7C%20osx--64-orange.svg)

*CNN-Diffusion-MRIBrain-Segmentation* repository is developed by Senthil Palanivelu, Suheyla Cetin Karayumak, Tashrif Billah, Sylvain Bouix, and Yogesh Rathi, Brigham and Women's Hospital (Harvard Medical School).

The CNN-Diffusion-MRIBrain-Segmentation-HCP repository was adapted from the CNN-Diffusion-MRIBrain-Segmentation Pipeline and is designed to run the HCP (Human Connectome Project) Masking Pipeline on a list of subjects. The pipeline involves several steps, including copying subject data from an S3 bucket, applying a brain masking process using a CNN model, and uploading the processed data back to the S3 bucket. The pipeline can be run in parallel using multiple cores, and it can process a specified number of subjects in each batch.

Running this adapted pipeline requires a GPU instance therefore we have used the g4dn.2xlarge ec2 instance with the updated AMI - Brainmasking_v9.16.2023.

### Table of Contents

* [Adapted Pipeline Outline](#adapted-pipeline-outline)
* [Prerequisites](#prerequisites)
* [Configuration File Setup](#configuration-file-setup)
* [Running the Pipeline](#running-the-pipeline)
* [Monitoring and Logs](#monitoring-and-logs)
* [Troubleshooting](#troubleshooting)
* [Contributing](#contributing)
* [License](#license)

## Adapted Pipeline Outline

1. Initialization: The pipeline is initialized with necessary configuration parameters, such as file paths, subject lists, and processing parameters.

2. Copying subject data: Subject data is copied from the S3 bucket to the local file system for processing.

3. Preparing input files: An input text file is created containing the list of subjects and their corresponding file paths for the CNN masking process.

4. Running the CNN masking process: A brain masking process is applied to the subject data using a CNN model.

5. Moving subject data to the 'processed' directory: The processed data is moved to a designated 'processed' directory.

6. Cleaning up additional files: Additional files generated during the processing are moved to a separate directory.

7. Uploading subject data to S3: The processed data is uploaded back to the S3 bucket.

8. Uploading additional files to S3: Additional files generated during the processing are also uploaded to the S3 bucket.

9. Verifying the upload and logging: The script verifies if the processed data has been successfully uploaded to the S3 bucket and logs the status.

10. Deleting local data: Local data is deleted before starting another batch of subjects.

11. Repeating the process: The pipeline runs in a loop, processing a specified number of subjects in each batch until all subjects have been processed.

The python script uses the argparse library to parse command-line arguments, or additionally arguments can be set in a configuration file, which makes running the script from the terminal clean and easy. It leverages the multiprocessing library to parallelize certain tasks as well to speed up the processing.

## Prerequisites

### Initial environment setup option 1: Using the prebuilt AMI

The adapted pipeline requires a GPU instance to run. We have used the g4dn.2xlarge ec2 instance with our prebuilt AMI - Brainmasking_v9.16.2023.

### Initial environment setup option 2: Building your own AMI

If you do not have access to our AMI, you can create your own AWS EC2 instance by creating a new g4dn.2xlarge instance which should use the `Deep Learning AMI GPU TensorFlow 2.13 (Amazon Linux 2)` and following the instructions in the [CNN-Masking-Pipeline instructions](docs/README.md) to set up the environment and install the necessary packages within the new instance.

### Additional packages for the adapted pipeline

* [S3path](https://github.com/liormizr/s3path): A Pythonic file-system interface to Amazon S3

S3path has been added as a dependency in the `environment_gpu.yml` file. It should be installed automatically when creating the environment from previous instructions.

If you are using a different environment, you can install the additional package by running the following command:

```bash
pip install s3path
```

## Configuration File Setup

The `hcp_config.ini` file is essential for setting up and running the masking pipeline. It contains various settings that control the behavior of the pipeline. Below is a detailed explanation of each section and variable in the configuration file. Additionally, these settings can be overridden by passing command-line arguments to the script.

### `[aws]` Section

* `s3_bucket_hcp_root`: Root directory of the HCP data on AWS S3.
  * **Example**: `s3://nda-enclave-c3371/HCP/`
  
* `caselist_file`: Location of the case list file.
  * **Example**: `/home/ec2-user/CNN-Diffusion-MRIBrain-Segmentation/pipeline/caselists/PDC_SUBJECTS.txt`

* `group_name`: Name of the group for the study.
  * **Example**: `PDC`

* `hcp_data_root`: Root directory for HCP data.
  * **Example**: `/data/HCP/`

* `dry_run`: Whether to perform a dry run (True/False).
  * **Example**: `True`

* `bids_study_root`: Root directory for BIDS study.
  * **Example**: `/data/HCP/`

* `log_loc`: Location for the processed log.
  * **Example**: `/home/ec2-user/logs/%(group_name)s/processed.log`

* `temp_log_loc`: Location for the temporary log.
  * **Example**: `/home/ec2-user/logs/%(group_name)s/temp.log`

* `start_index`: Starting index for processing.
  * **Example**: `1`

* `end_index`: Ending index for processing.
  * **Example**: `1000`

* `batch_size`: Batch size for processing.
  * **Example**: `50`

* `input_text`: Location of the input text file.
  * **Example**: `/home/ec2-user/process_list.txt`

* `model_folder`: Location of the model folder.
  * **Example**: `/home/ec2-user/model_folder`

* `additional_files_loc`: Location for additional files.
  * **Example**: `/data/HCP/processed/%(group_name)s/AdditionalFiles`

* `masking_script`: Location of the masking script.
  * **Example**: `/home/ec2-user/CNN-Diffusion-MRIBrain-Segmentation/pipeline/run_pipeline.py`

* `multiprocessing`: Whether to use multiprocessing (True/False).
  * **Example**: `True`

* `appendage`: Appendage for the output file name.
  * **Example**: `_MR`

* `file_substring`: Substring for the output file name.
  * **Example**: `_EdEp`

* `output_file_name`: Name of the output file.
  * **Example**: `harmonization`

To modify these settings, open the configuration file in a text editor and change the values accordingly. Save the file after making your changes.

#### Note on `batch_size`

The `batch_size`  will determine how many subjects will be downloaded and processed at a time. Thus you could have a caselist with 1000 subjects and you will only need enough storage to process the total subjects you set as the `batch_size`. After each batch the processed data will be uploaded to the S3 bucket and the local data will be deleted before starting the next batch.

#### Note on `caselist_file`

The `caselist_file` should contain a list of subject identifiers, one per line. These should not be full paths but rather just the unique identifiers for each subject you wish to process.

For example, the `caselist_file` might look like:

```txt
subject_001
subject_001
subject_003
...
```

This list will be used by the pipeline to identify which subjects to process. The subjects should each have their own folder located in the group folder which is specified with the `group_name` variable in the configuration file. The group folder should be located in the S3 bucket specified with the `s3_bucket_hcp_root` variable in the configuration file.

The subjects will be copied from the S3 bucket to the local EC2 instance and stored in the directory specified with the `hcp_data_root` variable in the configuration file. The subjects will be processed in the order they appear in the `caselist_file`.

This pipeline assumes  that the subjects are organized in the following way:

```txt
hpc_root/group_name/subject_001/unprocessed/Diffusion
```

The `Diffusion` folder should contain the AP and PA diffusion images for the subject. The `Diffusion` folder should also contain a `bvals` and `bvecs` file for the subject. The `bvals` and `bvecs` files should be named the same name as the diffusion image except with the `bvals` and `bvecs` extension, respectively.

The path to the config file can be set using the `--config_loc` flag when running the pipeline script. If no path is specified, the pipeline will look for the config file in the `CNN-Diffusion-MRIBrain-Segmentation/pipeline` directory. The default config file is `hcp_config.ini`.

## Running the Pipeline

There are two ways to run the pipeline:

1. Using a Screen Session
2. Running in the Background with `nohup`

### Shared Pre-requisites

Before running the pipeline in either mode, make sure to:

#### Activate the Conda Environment

Activate the conda environment where all the dependencies for the pipeline are installed:

```sh
conda activate dmri_seg
```

#### Navigate to the Pipeline Directory

Navigate to the directory containing the `hcp_masking_pipeline.py` script:

```sh
cd /path/to/CNN-Diffusion-MRIBrain-Segmentation/pipeline
```

### 1. Using a Screen Session

#### Starting a Screen Session

To start a new screen session, run:

```sh
screen -S hcp_masking_session
```

To detach from the screen session, press `Ctrl + A` followed by `D`.

To re-attach to the session, run:

```sh
screen -r hcp_masking_session
```

#### Running the Python Script

Now you can run the pipeline script:

```sh
# For a dry run to check if subjects are valid, or if `dry_run` is set to `True` in the config file you can leave out the `-dr` flag
python hcp_masking_pipeline.py --config_loc /path/to/hcp_config.ini -dr

# To actually run the pipeline, set `dry_run` to `False` in the config file or pass the `-r` flag
python hcp_masking_pipeline.py --config_loc /path/to/hcp_config.ini -r
```

### 2. Running in the Background with `nohup`

#### Running the Python Script in the Background

If you prefer not to use a screen session, you can run the script in the background and redirect its output to a log file:

```sh
nohup python hcp_masking_pipeline.py --config_loc /path/to/hcp_config.ini > /path/to/log/file.log 2>&1 &
```

**Note**: If `dry_run` is set to `True` in the config file and you wish to run the pipeline, you'll need to pass the `-r` flag to set it to `False`.

#### Viewing the Log File

To view the log file, run:

```sh
tail -f /path/to/log/file.log
```

## Monitoring and Logs

### Monitoring the Pipeline

To monitor the pipeline, you can use the `htop` command to view the CPU and memory usage of the pipeline script:

```sh
htop
```

### Viewing the Logs

The pipeline script will output two log files:

* `temp.log`: This log file contains the output from the pipeline script. It is overwritten each time the pipeline script is run.

* `processed.log`: This log file contains the output from the pipeline script. It is appended to each time the pipeline script is run.

The location of the log files can be set using the `temp_log_loc` and `log_loc` variables in the configuration file, respectively.

The processed log file will contain the following information for each subject:
    - The subject ID
    - Whether the subject was successfully processed or not
    - The time and date the subject was processed

The temp log is primarily used for saving the log from the s3 before appending the newly processed subjects to the processed log.

## Troubleshooting

If you run into any issues while running the pipeline, please check the following:
    - Make sure the prerequisites are installed and configured correctly
    - Make sure the configuration file is set up correctly
    - Make sure the caselist file is set up correctly
    - Make sure the subjects are organized correctly in the S3 bucket

If you are still having issues, please open an issue on GitHub or email the author [Ryan Zurrin](mailto:rzurrin@bwh.harvard.edu).

## Contributing

If you would like to contribute to this project, please feel free to submit a pull request or open an issue.

## License

This repository is licensed under the [MIT License](LICENSE).

# CNN-Diffusion-MRIBrain-Segmentation_HCP

## Adopted CNN Masking Pipeline Instructions for processing HCP data on AWS

![PNL BWH HMS logo](docs/pnl-bwh-hms.png "PNL BWH HMS logo")

[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.3665739.svg)](https://doi.org/10.5281/zenodo.3665739) ![Python](https://img.shields.io/badge/Python-3.6-green.svg)
![Platform](https://img.shields.io/badge/Platform-linux--64%20%7C%20osx--64-orange.svg)

*CNN-Diffusion-MRIBrain-Segmentation* repository is developed by Senthil Palanivelu, Suheyla Cetin Karayumak, Tashrif Billah, Sylvain Bouix, and Yogesh Rathi, 
Brigham and Women's Hospital (Harvard Medical School).

The CNN-Diffusion-MRIBrain-Segmentation-HCP repository was adapted from the CNN-Diffusion-MRIBrain-Segmentation Pipeline and is designed to run the HCP (Human Connectome Project) Masking Pipeline on a list of subjects. The pipeline involves several steps, including copying subject data from an S3 bucket, applying a brain masking process using a CNN model, and uploading the processed data back to the S3 bucket. The pipeline can be run in parallel using multiple cores, and it can process a specified number of subjects in each batch.

Running this adapted pipeline requires a GPU instance therefore we have used the g4dn.2xlarge ec2 instance with the updated AMI - Brainmasking_v4.22.2023.

### Table of Contents

* [Table of Contents](#table-of-contents)
  * [Initial environment setup](#initial-environment-setup)
  * [CNN-Diffusion-MRIBrain-Segmentatioin-HCP Outline](#adapted-pipeline-outline)


## Initial environment setup

Follow the original [CNN-Masking-Pipeline instructions](docs/README.md) to set up the environment. The adapted pipeline requires the following additional packages:


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

The python script uses the argparse library to parse command-line arguments, or additionally arguments can be set in a configuration file, which makes running the scrict from the terminal clean and easy. It leverages the multiprocessing library to parallelize certain tasks as well to speed up the processing.

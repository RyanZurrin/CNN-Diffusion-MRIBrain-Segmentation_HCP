#!/bin/bash

#aws s3 sync /home/ec2-user/CNN-Diffusion-MRIBrain-Segmentation_SuheylaChanges/ s3://nda-enclave-c3371/CNN-Diffusion-MRIBrain-Segmentation_SuheylaChanges/
for siteNo in {4..10};
do
	for batchNo in {1..6};
	do
		cd /home/ec2-user/nda_aws_token_generator/python/
		> ~/.aws/credentials
		python get_token_example.py
		cd /home/ec2-user/
    		DIR=/home/ec2-user/batch${batchNo}_site${siteNo}/
    		mkdir ${DIR}
    		if [ ! -d ${DIR}/sub-* ]; then
    		aws s3 sync s3://nda-enclave-c3371/abcdSites/site${siteNo}/target/Batch${batchNo}_site${siteNo}/ $DIR
    		fi

		if [ "$(ls -A $DIR)" ]; then
        		tarfile=/home/ec2-user//batch${batchNo}_site${siteNo}/Batch${batchNo}_site${siteNo}.txt
        		cd /home/ec2-user/batch${batchNo}_site${siteNo}/
			while read -r case
			do
	   			if [ -f ${case} ]; then
           			tar -xvzf ${case} > /dev/null
           			rm ${case}
	   			fi
        		done < ${tarfile}

			aws s3 sync /home/ec2-user/batch${batchNo}_site${siteNo}/ s3://nda-enclave-c3371/abcdSites/site${siteNo}/target/Batch${batchNo}_site${siteNo}/

			rm Batch${batchNo}_site${siteNo}_dwi.txt
			ls -ld sub-NDARINV*/*/*/*.bvec | awk '{print $NF}'  > abcd_tmp.txt
			sed 's/.bvec/.nii/' abcd_tmp.txt > abcd_tmp2.txt
			A="$(pwd)"
			B="/"
			while read -r case
        		do
            			printf '%s\n' $A$B${case} >>  Batch${batchNo}_site${siteNo}_dwi.txt

        		done < abcd_tmp2.txt

        		dwifile=/home/ec2-user//batch${batchNo}_site${siteNo}/Batch${batchNo}_site${siteNo}_dwi.txt
        		echo "Total cases = `cat $dwifile | wc -l`"


        		cd /home/ec2-user/CNN-Diffusion-MRIBrain-Segmentation_SuheylaChanges/pipeline/
        		python extractb0.py -i ${dwifile}

       		 	# delete dwi files to save space
        		while read -r case
        		do
            			rm ${case}

        		done < ${dwifile}

        		python antsRegistration.py -i ${dwifile} -f /home/ec2-user/model_folder/
        		python maskprocessing.py -i ${dwifile} -f /home/ec2-user/model_folder/
        		python postprocessing.py -i ${dwifile}

        		aws s3 sync /home/ec2-user/batch${batchNo}_site${siteNo}/ s3://nda-enclave-c3371/abcdSites/site${siteNo}/target/Batch${batchNo}_site${siteNo}/

			rm -r /home/ec2-user/batch${batchNo}_site${siteNo}/
    		else
        		echo "${batchNo} does not exist"
    		fi
	done
done

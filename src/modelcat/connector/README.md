# ModelCatConnector

## Introduction

This package provides commandline tools that validate and upload User datasets to the ModelCat Edge ML platform.

## Installation

ModelCatConnector is a subpackage of the modelcat package. Installation instructions available under the [modelcat README](../../../README.md).

## User Guide

Currently the following functions are supported:
* ModelCatConnector setup (one-time only)
* Dataset validation
* Dataset upload

### Dataset Preparation

In order to prepare your dataset, please follow our guide: [Dataset preparation guide](/docs/dataset_preparation.md)

### ModelCatConnector Setup

This one-time procedure will configure ModelCatConnector's access to the ModelCat platform. You will need to provide your `ModelCat Group ID` and `ModelCat OAuth Token`.

> **Note:** The OAuth token is used to authenticate with the ModelCat platform. You can obtain your OAuth token from the ModelCat platform. The token format should be an integer followed by an underscore, followed by a 40 character hexadecimal string (e.g., `1_1234567890abcdef1234567890abcdef12345678`).
>
> When you provide your OAuth token, ModelCatConnector will use it to authenticate with the ModelCat API and automatically retrieve temporary AWS credentials. These credentials are then configured in your AWS CLI profile for seamless access to ModelCat resources.

```
(modelcat_env) user@ubuntu: modelcat_setup
Welcome to ModelCatConnector one-time setup wizzard.
We'll get you started in just a few simple steps!
--------------------------------------------------
AWS CLI installation verified.
--------------------------------------------------
ModelCat Group ID: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXX
ModelCat OAuth Token: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--------------------------------------------------
Verifying AWS access...
Verification successful.
--------------------------------------------------
Configuration complete!

Now you can use:
        `modelcat_validate` to check your dataset for errors and verify ModelCat interoperability
        `modelcat_upload` to upload dataset to ModelCat platform

```
Once configuration is done, you can proceed with dataset validation and upload

### Dataset Validation
Validates user dataset to endure conformity with ModelCat dataset format. Any issues will be displayed to the user so that they can be correted prior to upload. If validation is passed succesfully your dataset will be signed and ready to upload. Note that you cannot upload the dataset without validating it first.
```
modelcat_validate
usage: modelcat_validate [-h] -d DATASET_PATH [--verbose]
```
Example usage:

```
(modelcat_env) user@ubuntu:~$ modelcat_validate -d ~/datasets/tf_test
                        ModelCatConnector (v0.0.1) - dataset validation utility

-------------------------------------------- Messages: ---------------------------------------------
WARNING: The dataset is missing the "thumbnail.jpg" file. Pick one image from the dataset, name it as "thumbnail.jpg" and place it inside the dataset root directory.
WARNING: There are 3 duplicate images (with same content, but different name) in your dataset. Check the "dataset validator log" file in the Dataset Analysis job to see details about those images.
WARNING: "dataset_infos.json" doesn't contain a valid entry for "num_examples" for split "train", 1222 images were found in the dataset root directory.

--------------------------------------------- Summary: ---------------------------------------------
No critical errors found
Creating dataset signature ...
Validation passed and signed: f3322dab7dccb61a66e611ee0dbaff1207b552b647bbb3d4066e6f953fc96ad1
```

### Dataset Upload

This utility will upload your dataset to ModelCat.

```
(modelcat_env) user@ubuntu:~$ modelcat_upload -d ~/datasets/tf_test
                        ModelCatConnector (v0.0.1) - dataset validation utility

----------------------------------------------------------------------------------------------------
Veryfying dataset signature...
Done!
S3 access verified
Found 3693 files in the dataset: 505.73 MB

Uploading file: tfds/tf_test-validation.tfrecord-00000-of-00001
100%|███████████████████████████████████████████████████████████| 3693/3693 [02:25<00:00, 25.46it/s]
----------------------------------------------------------------------------------------------------
Upload complete. You can view your dataset at: https://app.modelcat.ai/datasets/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXX/tf_test
```
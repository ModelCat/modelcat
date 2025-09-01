# Supported task types

As of today the ModelCatConnector utility supports the following task types:

* Image classification
* Object detection
* Keypoint detection

# Data preparation

Your dataset needs to be structured in a certain way to be properly recognized by ModelCat. All dataset items must be placed under a dataset root directory. Inside the dataset root you need to place following items:

* `dataset_infos.json` – high-level meta-description of your dataset
* `annotations/` - directory containing splits/annotations
* `images/` - directory containing images of your dataset

Together with this instruction you can find [dataset samples](../../../sample_datasets/) which demonstrate a correct way to prepare your data:

* `modelcat_classification_catsdogs_sample` – for classification
* `modelcat_objectdetection_sample` – for object detection
* `modelcat_keypoints_upperbody_sample` – for keypoints (detection)

## Dataset files layout
Below is an example directory tree for a dataset. Note that images can be placed in arbitrary locations, as long as they stay within the `images/` directory
```bash
your-dataset-name/	# E.g. cats-and-dogs

    dataset_infos.json # Meta description
    annotations/ 	# Split files with annotations in COCO format
        coco_test.json
        coco_train.json
        coco_validation.json
    images/  	# Images folder, an arbitrary sub-folder structure is allowed
        dir1/
            image1.jpg
            ...
        dir2/
            image3.jpg
            ...
        ...
        image2.jpg
        ...
```

~~~
Note: in naming files/directories please use alphanumeric file names and no special characters other than "-" and "_".
~~~

Dataset meta-file: `dataset_infos.json`

`dataset_infos.json` metafile contains high level description of your dataset, including task type definition, label names and references to split files. The layout of the file largely conforms to a HuggingFace format. Below, you can find examples for **classification**, **object detection** and **keypoint detection** tasks. 

~~~
Note: Don’t use any special characters in the dataset name except underscores and hyphens. Don’t use capital letters.
~~~

## Classification task

Example `dataset_infos.json` metafile for task classification for a cats and dogs dataset
```json
{
    "modelcat_classification_catsdogs_sample": { // the dataset name which will be visible on modelcat datasets page
        "description": "Sample dogs-vs-cats dataset\n", // Your dataset description
        "citation": "",
        "homepage": "https://app.modelcat.ai/",
        "license": "",
        "post_processed": null,
        "supervised_keys": null,
        "task_templates": [{
            "task": "classification", // Task type, in this example: classification
            "image_column": "image",
            "label_column": "label",
            "labels": ["cat", "dog"] // List of labels, order corresponding to categories in split/annotations files
        }],
        "builder_name": null, // Your dataset name
        "config_name": null, // Your dataset name
        "version": {
            "version_str": "1.0.0",
            "description": null,
            "major": 1,
            "minor": 0,
            "patch": 0
        },
        "splits": { // 3 splits are required – train, validation and test. 
                    // They contain reference to your split/annotation files
            "train": { // required
                "name": "train",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_train.json" // Required
            },
            "validation": { // Required
                "name": "validation",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_validation.json" // Required
            },
            "test": { // Required
                "name": "test",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_test.json" // Required
            }
        },
        "download_checksums": { },
        "download_size": null,
        "post_processing_size": null,
        "dataset_size": null,
        "size_in_bytes": null
    }
}
```

## Object detection task
Example below shows `dataset_infos.json` definition for object detection task. 
```json
{
    "modelcat_objectdetection_sample": {
        "description": "Subset of COCO-dataset. Contains colored images, where each image contains a set of objects of one class i.e. person. Each object has a corresponding bounding box in format [x_min, y_min, width, height]\n",
        "citation": "",
        "homepage": "https://app.modelcat.ai/",
        "license": "",
        "features": {
            "image": {
                "id": null,
                "_type": "Image"
            },
            "labels": {
                "id": null,
                "_type": "Sequence"
            },
            "image/filename": {
                "id": null,
                "_type": "Text"
            },
            "objects": {
                "id": null,
                "_type": "Sequence",
                "objects_bbox": {
                    "id": null,
                    "_type": "BBoxFeature"
                },
                "objects_label": {
                    "id": null,
                    "num_classes": 1,
                    "names": ["person"],
                    "_type": "ClassLabel"
                }
            }
        },
        "post_processed": null,
        "supervised_keys": null,
        "task_templates": [{
            "task": "detection", // Task type
            "image_column": "image",
            "label_column": null,
            "labels": ["person"] // Your labels
        }],
        "builder_name": "modelcat_objectdetection_sample",
        "config_name": "modelcat_objectdetection_sample",
        "version": {
            "version_str": "1.0.0",
            "description": null,
            "major": 1,
            "minor": 0,
            "patch": 0
        },
        "splits": { // 3 splits are required – train, validation and test
            "train": {
                "name": "train",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_train.json"
            },
            "validation": {
                "name": "validation",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_validation.json"
            },
            "test": {
                "name": "test",
                "num_bytes": null,
                "num_examples": null,
                "dataset_name": "coco_test.json"
            }
        },
        "download_size": null,
        "post_processing_size": null,
        "dataset_size": null,
        "size_in_bytes": null
    }
}
```

## Keypoints detection task

Example below shows `dataset_infos.json` definition for keypoints detection task. 
```json
{
    "modelcat_keypoints_upperbody_sample": {
        "description": "",
        "citation": "",
        "homepage": "https://app.modelcat.ai/",
        "license": "",
        "features": {
            "image": {
                "id": null,
                "_type": "Image"
            },
            "labels": {
                "id": null,
                "_type": "Sequence"
            },
            "image/filename": {
                "id": null,
                "_type": "Text"
            },
            "objects": {
                "id": null,
                "_type": "Sequence",
                "objects_bbox": {
                    "id": null,
                    "_type": "BBoxFeature"
                },
                "objects_label": {
                    "id": null,
                    "num_classes": 1,
                    "names": [
                        "person"
                    ],
                    "_type": "ClassLabel"
                },
                "objects_keypoint": {
                    "id": null,
                    "_type": "Sequence"
                }
            }
        },
        "post_processed": null,
        "supervised_keys": null,
        "task_templates": [
            {
                "task": "keypoints", // Task type
                "image_column": "image",
                "label_column": null,
                "labels": [ "person" ], // Your labels
                "annotations": [ // Fields from coco annotation json file that are required and used
                    "num_keypoints",
                    "area",
                    "iscrowd",
                    "keypoints",
                    "image_id",
                    "bbox",
                    "category_id",
                    "id"
                ],
                "num_keypoints": 8 // Total number of keypoints for each object
            }
        ],
        "builder_name": "modelcat_keypoints_upperbody_sample",
        "config_name": "modelcat_keypoints_upperbody_sample",
        "version": {
            "version_str": "1.0.1",
            "description": null,
            "major": 1,
            "minor": 0,
            "patch": 1
        },
        "splits": {  // 3 splits are required – train, validation and test
            "train": {
                "name": "train",
                "num_bytes": null,
                "num_examples": 1000,
                "dataset_name": "coco_train.json"
            },
            "validation": {
                "name": "validation",
                "num_bytes": null,
                "num_examples": 250,
                "dataset_name": "coco_validation.json"
            },
            "test": {
                "name": "test",
                "num_bytes": null,
                "num_examples": 250,
                "dataset_name": "coco_test.json"
            }
        },
        "download_size": null, // Total download size in bytes
        "post_processing_size": null,
        "dataset_size": null, // Total images size in bytes
        "size_in_bytes": null // Sum of download_size and dataset_size
    }
}
```

# Annotation files
ModelCat requires split files (train, validation and test) to be placed under the `/annotations` directory. Currently ModelCat supports the **COCO JSON** format, which means that split files will also contain annotations. To avoid mistakes it is suggested to use the following naming scheme for your split files: `coco_train.json`, `coco_validation.json` and `coco_test.json` for train, validation and test respectively as referenced in the example dataset metafile. Each annotation contains an image path, relative to the `images/` directory

Example split file for a classification task (`coco_test.json`)

```json
{
    "info": { // Boilerplate info, add any key-value information of choice
        "year": "2022",
        "version": "1.0",
        "description": "Converted from sample `dogs-vs-cats`",
        "contributor": "ModelCat",
        "url": "https://app.modelcat.ai",
        "date_created": "2022-12-16"
    },
    "licenses": [
        {
            "url": "https://app.modelcat.ai",
            "id": 1,
            "name": "MIT License"
        }
    ],
    "categories": [
        {
            "id": 1,
            "name": "cat",
            "supercategory": "animal"
        },
        {
            "id": 2,
            "name": "dog",
            "supercategory": "animal"
        }
    ],
    "images": [
        {
            "id": 1,
            "license": 1,
            "file_name": "cats/cat.5.jpg", // All paths are relative to the images/ directory
            "height": 144,
            "width": 175,
            "date_captured": "",
            "coco_url": "",
            "flickr_url": ""
        },
        {
            "id": 2,
            "license": 1,
            "file_name": "dogs/dog.8.jpg",
            "height": 500,
            "width": 469,
            "date_captured": "",
            "coco_url": "",
            "flickr_url": ""
        },
        ...
    ],
    "annotations": [
        {
            "id": 1,
            "image_id": 1, // Links to the image cats/cat.5.jpg
            "category_id": 1, // Category id 1 => cat
            "bbox": [],
            "segmentation": [
                []
            ],
            "iscrowd": null,
            "area": null,
            "dummy_tag": null
        },
        {
            "id": 2,
            "image_id": 2, // Links to the image dogs/dog.8.jpg
            "category_id": 2, // Category id 2 => dog
            "bbox": [],
            "segmentation": [
                []
            ],
            "iscrowd": null,
            "area": null,
            "dummy_tag": null
        },
        ...
    ]
}
```

~~~
NOTE: category id should match the order in which the category names are listed in the datasets_infos.json under labels key. First item in labels will have id=1, second id=2 etc. 
~~~

```json
// A fragment of dataset_infos.json 
    // Order of labels matters, they are implicitly counted from 1, that is: 
    // Cat => id=1
    // Dog => id=2
    "labels": ["cat", "dog"] 


// A fragment of coco_*.json
    "categories": [
        {
            "id": 1, // Id number could correspond to the position in dataset_infos.json, were cat id was set to 1
            "name": "cat",
            "supercategory": "animal" // Optional, leave "" or null
        },
        {
            "id": 2, // Id number could correspond to the position in dataset_infos.json, were dog id was set to 2
            "name": "dog",
            "supercategory": "animal"
        }
    ],
```

Example split file for object detection task (`coco_test.json`)

For object detection the file format is the same as for classification, with an addition of non-empty bbox key in the annotations. Bounding box coordinates are given in pixels in centroid format [x, y, width, height] where x and y denote the center of the bounding box.

```json
{
    ...
    "annotations": [
        {
            "id": 0,
            "image_id": 0,
            "category_id": 1,
            "bbox": [ // In addition to category_id bounding box is specified 
                      // Bounding box coordinates are given in the [x, y, width, height] format
                211,
                155,
                16,
                45
            ],
            "segmentation": [
                []
            ],
            "iscrowd": null,
            "area": null,
            "dummy_tag": null
        },
        {
            "id": 1,
            "image_id": 1,
            "category_id": 1,
            "bbox": [
                0,
                84,
                514,
                548
            ],
            "segmentation": [
                []
            ],
            "iscrowd": null,
            "area": null,
            "dummy_tag": null
        },
        ...
    ]
}
```

Example split file for keypoints task (`coco_test.json`)

```json
{
    "info": {
        "year": "2023",
        "version": "1.0.0",
        "description": "Single Image Single Person Dataset with Upper Body Keypoints - Source: COCO 2014 Val Dataset",
        "contributor": "ModelCat",
        "url": "https://app.modelcat.ai",
        "date_created": "2023-07-14"
    },
    "licenses": [
        {
            "url": "https://app.modelcat.ai",
            "id": 1,
            "name": "MIT License"
        }
    ],
    "categories": [
        {
            "supercategory": "person",
            "name": "person",
            "skeleton": [ [ 7, 5 ], [ 8, 6 ], [ 5, 6 ], [ 1, 5 ], [ 2, 6 ], [ 1, 2 ], [ 1, 3 ], [ 2, 4 ] ], // Keypoints connections for visualizations
            "keypoints": [
                "left_shoulder", "right_shoulder", "left_elbow", "right_elbow", "left_hip", "right_hip", "left_knee", "right_knee" ], // Names of skeleton connections
            "id": 1 // Category id for each label corresponds to `category_id` value in annotations
        }
    ],
    "images": [
        {
            "license": 1,
            "file_name": "image_1251.jpg", // Image path relative to `images` directory
            "height": 335,
            "width": 315,
            "id": 1251 // Unique image id
        },
        {
            "license": 1,
            "file_name": "image_1252.jpg",
            "height": 243,
            "width": 162,
            "id": 1252
        },
        ...
    ],
    "annotations": [
        {
            "num_keypoints": 8, // Number of visible keypoints
            "area": 25752.0151,
            "iscrowd": 0,
            "keypoints": [ 185, 80, 2, 120, 69, 2, 221, 125, 2, 72, 90, 2, 151, 180, 2, 106, 175, 2, 145, 266, 2, 94, 261, 2 ], // x1, y1, v1, x2, y2, v2 ... x=x coordinate, y=y coordinate, v=visibility
            "image_id": 1251,
            "bbox": [ 18, 6, 291, 328 ], // Bounding box coordinates in [x, y, width, height] format
            "category_id": 1,
            "id": 1251
        },
        {
            "num_keypoints": 8,
            "area": 14344.14265,
            "iscrowd": 0,
            "keypoints": [ 54, 45, 1, 100, 52, 2, 32, 68, 2, 121, 93, 2, 36, 105, 2, 72, 108, 2, 72, 160, 2, 98, 153, 2 ],
            "image_id": 1252,
            "bbox": [ 5, 14, 152, 215 ],
            "category_id": 1,
            "id": 1252
        },
        ...
}
```

The "keypoints" in annotations is of `3*num_keypoints` in length (total keypoints for an object regardless of visibility) array `(x, y, v)` for object keypoints. Each keypoint has a 0-indexed location `x,y` and a visibility flag `v` defined as:
* `v=0`: not labeled (in which case `x=y=0`), 
* `v=1`: labeled but not visible
* `v=2`: labeled and visible. The `num_keypoints` in annotations is the number of visible keypoints not the total keypoints, total keypoints are given in "num_keypoints" key in "task_templates" inside `dataset_infos.json`.

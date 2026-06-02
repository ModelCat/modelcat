import argparse
import json
import logging as log
import os
import re
import shutil
import subprocess
import sys
from os import environ

# Setup logging
log.basicConfig(format="%(levelname)s: %(message)s", level=log.INFO)


def ensure_dependencies():
    """Checks for required dependencies and installs them if missing."""
    required_packages = {
        "roboflow": "roboflow",
        "dotenv": "python-dotenv",
        "requests": "requests",
    }
    missing = [
        pkg
        for mod, pkg in required_packages.items()
        if not __import__("importlib.util").util.find_spec(mod)
    ]
    if missing:
        log.info(f"Installing missing dependencies: {', '.join(missing)}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        except subprocess.CalledProcessError as e:
            log.error(f"Failed to install dependencies: {e}")
            sys.exit(1)


def parse_roboflow_url(url: str):
    """Extracts workspace, project, and version from a Roboflow Universe URL."""
    pattern = r"universe\.roboflow\.com/([^/]+)/([^/]+)(?:/(?:dataset|model)/)?(\d+)?"
    match = re.search(pattern, url)
    if not match:
        raise ValueError(
            "Invalid Roboflow Universe URL. Expected format: https://universe.roboflow.com/<workspace>/<project>[/version]"
        )

    workspace, project, version = match.groups()
    return workspace, project, int(version) if version else None


def get_api_key():
    """Fetches the API key from .env or prompts the user interactively."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    api_key = environ.get("ROBOFLOW_API_KEY")
    if not api_key:
        print("\nRoboflow API key not found in .env.")
        print(
            "You can get your API key from https://app.roboflow.com/ -> Settings -> API Keys"
        )
        api_key = input("Enter your Roboflow API Key: ").strip()
        if not api_key:
            log.error("API Key is required to fetch datasets.")
            sys.exit(1)

        save = input("Save this key to .env for future use? (y/n): ").strip().lower()
        if save == "y":
            with open(".env", "a") as f:
                f.write(f"\nROBOFLOW_API_KEY={api_key}\n")
            log.info("API Key saved to .env")
    return api_key


def clean_supercategories(base_dir):
    """Removes supercategories and empty classes."""
    # Catch both 'valid' and 'val' names
    splits = ["train", "valid", "val", "test"]
    anns_by_split = {}
    for split in splits:
        p = os.path.join(base_dir, split, "_annotations.coco.json")
        if os.path.exists(p):
            with open(p, "r") as f:
                anns_by_split[split] = json.load(f)
        else:
            anns_by_split[split] = None

    all_categories = []
    for data in anns_by_split.values():
        if data:
            all_categories.extend(data.get("categories", []))

    has_supercategories = any(
        c.get("supercategory", "none") != "none" and c.get("supercategory") is not None
        for c in all_categories
    )

    category_counts = {}
    category_info = {}

    for data in anns_by_split.values():
        if not data:
            continue
        for cat in data.get("categories", []):
            cat_id = cat["id"]
            sc = cat.get("supercategory", "none")
            keep_category = not (has_supercategories and (sc == "none" or sc is None))
            if keep_category and cat_id not in category_info:
                category_info[cat_id] = cat
                category_counts[cat_id] = 0

    for data in anns_by_split.values():
        if not data:
            continue
        for ann in data.get("annotations", []):
            cat_id = ann["category_id"]
            if cat_id in category_counts:
                category_counts[cat_id] += 1

    kept_ids = sorted([cid for cid, count in category_counts.items() if count > 0])
    id_map = {old_id: new_idx for new_idx, old_id in enumerate(kept_ids)}

    for split, data in anns_by_split.items():
        if not data:
            continue
        new_categories = []
        for cat in data.get("categories", []):
            if cat["id"] in id_map:
                new_cat = cat.copy()
                new_cat["id"] = id_map[cat["id"]]
                new_cat["supercategory"] = "none"
                new_categories.append(new_cat)
        data["categories"] = sorted(new_categories, key=lambda x: x["id"])

        new_annotations = []
        for ann in data.get("annotations", []):
            if ann["category_id"] in id_map:
                new_ann = ann.copy()
                new_ann["category_id"] = id_map[ann["category_id"]]
                new_annotations.append(new_ann)
        data["annotations"] = new_annotations

        with open(os.path.join(base_dir, split, "_annotations.coco.json"), "w") as f:
            json.dump(data, f)


def format_for_modelcat(rf_dir, dest_dir, project_name):
    """Restructures Roboflow export to ModelCat standard."""
    log.info("Converting dataset to ModelCat format...")
    os.makedirs(dest_dir, exist_ok=True)
    os.makedirs(os.path.join(dest_dir, "annotations"), exist_ok=True)
    os.makedirs(os.path.join(dest_dir, "images"), exist_ok=True)

    # Map all Roboflow variations to ModelCat splits
    rf_splits = {
        "train": "train",
        "valid": "validation",
        "val": "validation",
        "test": "test",
    }
    rf_license = None

    for rf_split, mc_split in rf_splits.items():
        src_split_dir = os.path.join(rf_dir, rf_split)
        if not os.path.exists(src_split_dir):
            continue

        mc_img_dir = os.path.join(dest_dir, "images", mc_split)
        if not os.path.exists(mc_img_dir):
            shutil.copytree(
                src_split_dir, mc_img_dir, ignore=shutil.ignore_patterns("*.json")
            )

        ann_path = os.path.join(src_split_dir, "_annotations.coco.json")
        if os.path.exists(ann_path):
            with open(ann_path, "r") as f:
                coco_data = json.load(f)

            if not rf_license and coco_data.get("licenses"):
                rf_license = coco_data["licenses"][0]

            for image in coco_data.get("images", []):
                image["file_name"] = f"{mc_split}/{image['file_name']}"
            for cat in coco_data.get("categories", []):
                cat["id"] += 1
            for annotation in coco_data.get("annotations", []):
                annotation["category_id"] += 1

            dest_ann_path = os.path.join(
                dest_dir, "annotations", f"coco_{mc_split}.json"
            )
            with open(dest_ann_path, "w") as f:
                json.dump(coco_data, f)

    return rf_license


def generate_dataset_infos(dest_dir, project_name, rf_license, task_type="detection"):
    """Generates the metadata JSON required by ModelCat."""
    license_name = rf_license.get("name", "Unknown") if rf_license else "Unknown"

    if license_name and any(
        x in license_name.upper() for x in ["NC", "NON-COMMERCIAL", "GPL"]
    ):
        log.warning(
            f"ATTENTION: Dataset has a strictly commercial or restrictive license: {license_name}"
        )

    class_names = []
    for split in ["train", "validation", "test"]:
        ann_path = os.path.join(dest_dir, "annotations", f"coco_{split}.json")
        if os.path.exists(ann_path):
            with open(ann_path) as f:
                coco_data = json.load(f)
                class_names = [
                    c["name"]
                    for c in sorted(
                        coco_data.get("categories", []), key=lambda x: x["id"]
                    )
                ]
                break

    splits = {}
    total_imgs, total_bytes = 0, 0

    for split in ["train", "validation", "test"]:
        img_dir = os.path.join(dest_dir, "images", split)
        ann_path = os.path.join(dest_dir, "annotations", f"coco_{split}.json")

        if (
            os.path.exists(img_dir)
            and os.path.exists(ann_path)
            and len(os.listdir(img_dir)) > 0
        ):
            num_imgs = len(os.listdir(img_dir))
            num_bytes = sum(
                os.path.getsize(os.path.join(img_dir, f)) for f in os.listdir(img_dir)
            )
            splits[split] = {
                "name": split,
                "num_bytes": num_bytes,
                "num_examples": num_imgs,
                "dataset_name": f"coco_{split}.json",
            }
            total_imgs += num_imgs
            total_bytes += num_bytes
        else:
            empty_coco = {
                "info": {},
                "licenses": [],
                "images": [],
                "annotations": [],
                "categories": [],
            }
            os.makedirs(os.path.join(dest_dir, "annotations"), exist_ok=True)
            os.makedirs(os.path.join(dest_dir, "images", split), exist_ok=True)

            empty_ann_path = os.path.join(dest_dir, "annotations", f"coco_{split}.json")
            with open(empty_ann_path, "w") as f:
                json.dump(empty_coco, f)

            splits[split] = {
                "name": split,
                "num_bytes": 0,
                "num_examples": 0,
                "dataset_name": f"coco_{split}.json",
            }

    dataset_infos = {
        project_name: {
            "description": f"Roboflow dataset fetched via modelcat_fetch. License: {license_name}",
            "license": str(rf_license) if rf_license else "",
            "builder_name": project_name,
            "config_name": project_name,
            "dataset_size": total_imgs,
            "size_in_bytes": total_bytes,
            "splits": splits,
            "task_templates": [
                {
                    "task": task_type,
                    "image_column": "image",
                    "label_column": None,
                    "labels": class_names,
                }
            ],
        }
    }

    with open(os.path.join(dest_dir, "dataset_infos.json"), "w", encoding="utf-8") as f:
        json.dump(dataset_infos, f, indent=4)


def fetch_cli():
    parser = argparse.ArgumentParser(
        description="Fetch and format datasets from Roboflow Universe to ModelCat."
    )
    parser.add_argument(
        "save_path", help="Local directory to save the ModelCat dataset."
    )
    parser.add_argument("--url", required=True, help="Roboflow Universe URL")
    args = parser.parse_args()

    ensure_dependencies()
    from roboflow import Roboflow

    try:
        workspace_name, project_name, version_num = parse_roboflow_url(args.url)
    except ValueError as e:
        log.error(e)
        sys.exit(1)

    api_key = get_api_key()

    dest_dir = os.path.abspath(args.save_path)
    if os.path.exists(dest_dir) and os.listdir(dest_dir):
        log.error(
            f"Save path '{dest_dir}' already exists and is not empty. Choose an empty directory."
        )
        sys.exit(1)

    rf = Roboflow(api_key=api_key)
    try:
        workspace = rf.workspace(workspace_name)
        project = workspace.project(project_name)

        if version_num is None:
            log.info("No version specified in URL. Scanning for the latest version...")
            try:
                versions = project.versions()
                version_num = max([int(v.version) for v in versions])
                log.info(f"Auto-selected latest version: v{version_num}")
            except Exception as e:
                log.warning(
                    f"Could not auto-detect latest version, defaulting to 1. Details: {e}"
                )
                version_num = 1

        version = project.version(version_num)

        rf_type = project.type
        task_mapping = {
            "object-detection": "detection",
            "classification": "classification",
            "single-label-classification": "classification",
            "multi-label-classification": "classification",
            "keypoint-detection": "keypoints",
        }

        if rf_type not in task_mapping:
            log.error(
                f"Task type '{rf_type}' is currently unsupported by ModelCat. Supported: detection, classification, keypoints."
            )
            sys.exit(1)

        mc_task_type = task_mapping[rf_type]

    except Exception as e:
        log.error(f"Failed to fetch project metadata from Roboflow. Details: {e}")
        sys.exit(1)

    log.info(
        f"Detected task: {mc_task_type}. Fetching {project_name} v{version_num}..."
    )

    # IMPORTANT: Do NOT use os.makedirs here. If the folder exists, the SDK skips the download!
    tmp_dir = os.path.abspath("./.rf_temp_download")
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    try:
        version.download("coco", location=tmp_dir)
        source_data_path = tmp_dir
        log.info(f"Download complete. Extracting from: {source_data_path}")

        if mc_task_type == "detection":
            log.info("Cleaning supercategories...")
            clean_supercategories(source_data_path)

        rf_license = format_for_modelcat(source_data_path, dest_dir, project_name)
        generate_dataset_infos(dest_dir, project_name, rf_license, mc_task_type)

        total_images_found = sum(
            [len(files) for r, d, files in os.walk(os.path.join(dest_dir, "images"))]
        )
        if total_images_found == 0:
            log.error(
                f"CRITICAL: The downloaded Roboflow dataset (v{version_num}) contains 0 images."
            )
            log.error(
                "PROPOSED FIX: This often happens on Roboflow when a version is generated before images are uploaded."
            )
            log.error(
                "Please verify the project on Roboflow Universe and append the correct version to your URL."
            )
            shutil.rmtree(dest_dir)
            sys.exit(1)

        thumbnail_path = os.path.join(dest_dir, "thumbnail.jpg")
        if not os.path.exists(thumbnail_path):
            log.info("Generating dataset thumbnail...")
            found_thumbnail = False
            for root, dirs, files in os.walk(os.path.join(dest_dir, "images")):
                for file in files:
                    if file.lower().endswith((".png", ".jpg", ".jpeg", ".bmp")):
                        shutil.copyfile(os.path.join(root, file), thumbnail_path)
                        found_thumbnail = True
                        break
                if found_thumbnail:
                    break

        log.info(f"Success! Dataset formatted and saved to: {dest_dir}")
        log.info(f"You can now run: modelcat_validate -d {dest_dir}")

    except Exception as e:
        log.exception(f"An error occurred during dataset processing: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    fetch_cli()

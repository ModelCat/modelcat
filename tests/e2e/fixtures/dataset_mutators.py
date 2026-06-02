import json
import os
import os.path as osp
import shutil
from typing import Any, List, Optional


def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def remove_file(base_dir: str, relative_path: str) -> None:
    target = osp.join(base_dir, relative_path)
    if osp.isfile(target):
        os.remove(target)
    elif osp.isdir(target):
        shutil.rmtree(target)


def _resolve_key(key: str):
    """Convert a string key to int if it looks like an array index."""
    try:
        return int(key)
    except ValueError:
        return key


def remove_json_key(json_path: str, dotted_key: str) -> None:
    """
    Remove a key from a JSON file using dot notation.
    Supports numeric indices for arrays, e.g. "task_templates.0.labels".
    """
    data = load_json(json_path)
    keys = dotted_key.split(".")
    obj = data
    for key in keys[:-1]:
        obj = obj[_resolve_key(key)]
    del obj[_resolve_key(keys[-1])]
    save_json(json_path, data)


def set_json_value(json_path: str, dotted_key: str, value: Any) -> None:
    """Set a value in a JSON file using dot notation. Supports numeric indices."""
    data = load_json(json_path)
    keys = dotted_key.split(".")
    obj = data
    for key in keys[:-1]:
        obj = obj[_resolve_key(key)]
    obj[_resolve_key(keys[-1])] = value
    save_json(json_path, data)


def corrupt_json(json_path: str) -> None:
    """Write invalid JSON to a file."""
    with open(json_path, "w") as f:
        f.write("{this is not valid json!!!")


def remove_coco_image_field(
    ann_path: str, field_name: str, image_index: int = 0
) -> None:
    """Remove a field from a specific image entry in a COCO annotation file."""
    data = load_json(ann_path)
    if field_name in data["images"][image_index]:
        del data["images"][image_index][field_name]
    save_json(ann_path, data)


def remove_coco_image_field_all(ann_path: str, field_name: str) -> None:
    """Remove a field from ALL image entries in a COCO annotation file."""
    data = load_json(ann_path)
    for img in data["images"]:
        img.pop(field_name, None)
    save_json(ann_path, data)


def remove_coco_section(ann_path: str, section: str) -> None:
    """Remove an entire section (categories/images/annotations) from a COCO file."""
    data = load_json(ann_path)
    data.pop(section, None)
    save_json(ann_path, data)


def empty_split(ann_path: str) -> None:
    """Empty a split by clearing its images and annotations (categories kept)."""
    data = load_json(ann_path)
    data["images"] = []
    data["annotations"] = []
    save_json(ann_path, data)


def remove_coco_annotation_field(
    ann_path: str, field_name: str, annotation_index: int = 0
) -> None:
    """Remove a field from a specific annotation entry."""
    data = load_json(ann_path)
    if field_name in data["annotations"][annotation_index]:
        del data["annotations"][annotation_index][field_name]
    save_json(ann_path, data)


def remove_coco_category_field(
    ann_path: str, field_name: str, category_index: int = 0
) -> None:
    """Remove a field from a specific category entry."""
    data = load_json(ann_path)
    if field_name in data["categories"][category_index]:
        del data["categories"][category_index][field_name]
    save_json(ann_path, data)


def add_duplicate_image_entry(ann_path: str) -> None:
    """Duplicate the first image entry in a COCO file (same file_name, new id)."""
    data = load_json(ann_path)
    if data["images"]:
        dup = dict(data["images"][0])
        max_id = max(img["id"] for img in data["images"])
        dup["id"] = max_id + 1000
        data["images"].append(dup)
    save_json(ann_path, data)


def add_duplicate_category_id(ann_path: str) -> None:
    """Create a duplicate category ID by copying the first category."""
    data = load_json(ann_path)
    if data["categories"]:
        dup = dict(data["categories"][0])
        dup["name"] = dup["name"] + "_dup"
        data["categories"].append(dup)
    save_json(ann_path, data)


def add_duplicate_annotation_id(ann_path: str) -> None:
    """Create a duplicate annotation ID by copying the first annotation."""
    data = load_json(ann_path)
    if data["annotations"]:
        dup = dict(data["annotations"][0])
        data["annotations"].append(dup)
    save_json(ann_path, data)


def add_orphan_annotation(ann_path: str) -> None:
    """Add an annotation referencing a non-existent image_id."""
    data = load_json(ann_path)
    max_ann_id = max(a["id"] for a in data["annotations"]) if data["annotations"] else 0
    max_img_id = max(img["id"] for img in data["images"]) if data["images"] else 0
    cat_id = data["categories"][0]["id"] if data["categories"] else 1
    data["annotations"].append({
        "id": max_ann_id + 9999,
        "image_id": max_img_id + 9999,
        "category_id": cat_id,
        "bbox": [0, 0, 10, 10],
        "area": 100,
        "iscrowd": 0,
    })
    save_json(ann_path, data)


def add_orphan_category_annotation(ann_path: str) -> None:
    """Add an annotation referencing a non-existent category_id."""
    data = load_json(ann_path)
    max_ann_id = max(a["id"] for a in data["annotations"]) if data["annotations"] else 0
    max_cat_id = max(c["id"] for c in data["categories"]) if data["categories"] else 0
    img_id = data["images"][0]["id"] if data["images"] else 1
    data["annotations"].append({
        "id": max_ann_id + 9999,
        "image_id": img_id,
        "category_id": max_cat_id + 9999,
        "bbox": [0, 0, 10, 10],
        "area": 100,
        "iscrowd": 0,
    })
    save_json(ann_path, data)


def mismatch_category_labels(ann_path: str) -> None:
    """Rename the first category so it no longer matches dataset_infos labels."""
    data = load_json(ann_path)
    if data["categories"]:
        data["categories"][0]["name"] = "nonexistent_label_xyz"
    save_json(ann_path, data)


def inject_split_leakage(ann_path_1: str, ann_path_2: str, count: int = 5) -> None:
    """
    Copy `count` images from ann_path_1 into ann_path_2 to create split leakage.
    """
    data1 = load_json(ann_path_1)
    data2 = load_json(ann_path_2)
    leaked = data1["images"][:count]
    for img in leaked:
        if not any(i["file_name"] == img["file_name"] for i in data2["images"]):
            new_img = dict(img)
            max_id = max(i["id"] for i in data2["images"]) if data2["images"] else 0
            new_img["id"] = max_id + 1
            data2["images"].append(new_img)
    save_json(ann_path_2, data2)


def remove_image_file(images_dir: str, coco_path: str, image_index: int = 0) -> Optional[str]:
    """
    Remove an actual image file from disk that is referenced in the COCO annotation.
    Returns the file_name that was removed.
    """
    data = load_json(coco_path)
    if not data["images"]:
        return None
    file_name = data["images"][image_index]["file_name"]
    full_path = osp.join(images_dir, file_name)
    if osp.exists(full_path):
        os.remove(full_path)
    return file_name


def add_image_without_annotation(
    images_dir: str, ann_path: str, fake_name: str = "orphan_image.jpg"
) -> None:
    """
    Add an image entry to the COCO file that has no corresponding annotation,
    and create a minimal JPEG file on disk.
    """
    data = load_json(ann_path)
    max_id = max(img["id"] for img in data["images"]) if data["images"] else 0
    data["images"].append({
        "id": max_id + 8888,
        "file_name": fake_name,
        "width": 10,
        "height": 10,
    })
    save_json(ann_path, data)

    img_path = osp.join(images_dir, fake_name)
    os.makedirs(osp.dirname(img_path), exist_ok=True)
    try:
        from PIL import Image
        img = Image.new("RGB", (10, 10), color="red")
        img.save(img_path, "JPEG")
    except ImportError:
        with open(img_path, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0" + b"\x00" * 100)


def get_first_annotation_file(dataset_dir: str) -> Optional[str]:
    """Return the path to the first annotation file found."""
    ann_dir = osp.join(dataset_dir, "annotations")
    if not osp.isdir(ann_dir):
        return None
    for f in sorted(os.listdir(ann_dir)):
        if f.endswith(".json"):
            return osp.join(ann_dir, f)
    return None


def get_all_annotation_files(dataset_dir: str) -> List[str]:
    """Return paths to all annotation JSON files."""
    ann_dir = osp.join(dataset_dir, "annotations")
    if not osp.isdir(ann_dir):
        return []
    return sorted(
        osp.join(ann_dir, f)
        for f in os.listdir(ann_dir)
        if f.endswith(".json")
    )

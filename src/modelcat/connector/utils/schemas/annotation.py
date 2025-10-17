from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator, root_validator

"""
This file contains validation schemas for COCO-style annotations

Does not do:
    1. Check path of image files if they exist.
"""

class Info(BaseModel):
    """
    COCO-style dataset 'info' metadata.

    Attributes:
        year: Dataset year (free-form string, e.g., "2021").
        version: Dataset version string (e.g., "1.0").
        description: High-level description of the dataset.
        contributor: Dataset contributor or organization name.
        url: URL to the dataset homepage or documentation.
        date_created: Creation date string (format not enforced).

    All fields are optional and arbitrary extra keys are allowed.
    """
    year: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    contributor: Optional[str] = None
    url: Optional[str] = None
    date_created: Optional[str] = None

    class Config:
        # any extra field is allowed
        extra = "allow"


class License(BaseModel):
    """
    COCO-style license entry describing how images/annotations may be used.

    Attributes:
        id: Unique integer ID of this license entry.
        name: Human-readable license name (e.g., "CC-BY-4.0").
        url: Link to the full license text.
    """
    id: int
    name: Optional[str] = None
    url: Optional[str] = None

    class Config:
        extra = "allow"
        

class Category(BaseModel):
    """
    COCO-style category (class) definition.

    Attributes:
        id: Unique integer category ID (referenced by annotations).
        name: Category name (e.g., "person", "car").
        supercategory: Optional broader grouping (e.g., "vehicle").
        keypoints: Optional ordered list of keypoint labels (for pose tasks).
            If present, annotations for this category are expected to carry keypoints in x,y,v triplets.
        skeleton: Optional list of [i, j] **1-based** pairs indicating keypoint connections
            (used by some tools for visualization). Indices must satisfy 1 <= i,j <= len(keypoints).
    """
    
    id: int
    name: str
    supercategory: str
    keypoints: Optional[List[str]] = None  # keypoint labels
    skeleton: Optional[List[List[int]]] = None   # list of [from, to] keypoint connections

    class Config:
        extra = "allow"

    @validator("keypoints")
    def _keypoints_when_provided(cls, v):
        """
        Validate that 'keypoints'—when present—is a clean list of non-empty strings.

        Returns:
            The original list if valid, or None if not provided.

        Raises:
            ValueError: If the provided value is not a list of non-empty strings.
        """
        if v is None:
            return v
        if not isinstance(v, list):
            raise ValueError("keypoints must be a list of strings when provided.")
        if any(not isinstance(k, str) for k in v):
            raise ValueError("keypoints must be strings.")
        if any(not k.strip() for k in v):
            raise ValueError("keypoints entries must be non-empty strings.")
        return v

    @validator("skeleton")
    def _skeleton_when_provided(cls, v, values):
        """
        Validate that 'skeleton'—when present:
          - is a list of [i, j] pairs of ints,
          - uses 1-based indices, and
          - each index falls within [1, len(keypoints)].

        Raises:
            ValueError: on bad structure, non-integers, 0/negative indices,
                out-of-range indices, or if keypoints is missing when skeleton is provided.
        """
        if v is None:
            return v
        
        # structural checks
        if not isinstance(v, list):
            raise ValueError("skeleton must be a list of [i, j] pairs when provided.")
        for pair in v:
            if not (isinstance(pair, list) and len(pair) == 2):
                raise ValueError("each skeleton entry must be a 2-item list [i, j].")
            i, j = pair
            if not (isinstance(i, int) and isinstance(j, int)):
                raise ValueError("skeleton indices must be integers.")
            
        keypoints = values.get("keypoints")
        num_kp = len(keypoints) if keypoints else None

        # enforce 1-based indexing and range [1, n]
        for i, j in v:
            if i < 1 or j < 1:
                raise ValueError(
                    f"skeleton indices must be 1-based; got [{i}, {j}] (indices must be >= 1)."
                )
            if num_kp is not None and (i > num_kp or j > num_kp):
                raise ValueError(
                    f"skeleton indices out of range for {num_kp} keypoints; got [{i}, {j}] (must be <= {num_kp})."
                )
        
        return v
        

class Image(BaseModel):
    """
    COCO-style image metadata entry.

    Attributes:
        id: Unique integer ID of the image (referenced by annotations).
        file_name: Image filename (e.g., "/path/to/000000000001.jpg").
        height: Image height in pixels.
        width: Image width in pixels.
        license: Optional integer referencing a License.id.
        date_captured: Optional capture timestamp string.
        coco_url: Optional URL to COCO-hosted image (if applicable).
        flickr_url: Optional Flickr URL (if applicable).
    """

    id: Union[int, float, str]
    file_name: str
    height: Optional[int] = None
    width: Optional[int] = None
    license: Optional[int] = None
    date_captured: Optional[str] = None
    coco_url: Optional[str] = None
    flickr_url: Optional[str] = None

    class Config:
        extra = "allow"
        
        
class Annotation(BaseModel):
    """
    COCO-style annotation for an object instance (and optionally keypoints).

    Attributes:
        id: Unique integer annotation ID.
        image_id: Foreign key to Image.id.
        category_id: Foreign key to Category.id.
        bbox: Optional [x, y, width, height] with non-negative numbers.
        segmentation: Optional polygons/masks; here modeled as List[List[float|int]].
        iscrowd: Optional flag for crowd regions (0 or 1).
        area: Optional scalar area value.
        keypoints: Optional flat list for pose tasks:
                   [x0, y0, v0, x1, y1, v1, ..., xK-1, yK-1, vK-1]
                   where v must be {0,1,2} is visibility (COCO convention).
        num_keypoints: Optional count of visible keypoints (v > 0).
    """

    id: Union[int, float, str]
    image_id: Union[int, float, str]
    category_id: int
    bbox: Optional[List[Union[int, float]]] = Field(default_factory=list)
    segmentation: Optional[List[List[Union[int, float]]]] = None
    iscrowd: Optional[int] = None  # allow None/0/1
    area: Optional[Union[int, float]] = None

    # optional keypoints for pose tasks
    keypoints: Optional[List[Union[int, float]]] = None
    num_keypoints: Optional[int] = None

    class Config:
        extra = "allow"

    @validator("bbox")
    def _check_bbox(cls, v, values):
        """
        Validate 'bbox' when supplied:
        - If missing or empty: allowed (some tasks omit bbox).
        - If present: must be [x, y, width, height] with non-negative numerics.

        Returns:
            The original bbox if valid, or None/[] if not provided.

        Raises:
            ValueError: If bbox has wrong length/type or negative values.
        """
        ann_id = values.get("id", "<unknown>")  # for error messages

        # replace None with [] for uniform downstream handling
        if v is None:
            return []
        
        # empty bbox is acceptable
        if len(v) == 0:
            return v

        # if not empty, must be exactly 4 elements [x, y, width, height]
        if len(v) != 4:
            raise ValueError(f"[ann id={ann_id}] bbox must have 4 elements [x, y, width, height] when provided.")

        # validate type and non-negativity
        x, y, w, h = v
        for val in (x, y, w, h):
            if not isinstance(val, (int, float)):
                raise ValueError(f"[ann id={ann_id}] bbox values must be numeric.")
            if val < 0:
                raise ValueError(f"[ann id={ann_id}] bbox values must be non-negative.")
        return v
    
    @validator("keypoints")
    def _check_keypoints(cls, v, values):
        ann_id = values.get("id", "<unknown>")  # for error messages

        if v is None:
            return v
        if not isinstance(v, list) or (len(v) % 3) != 0:
            raise ValueError(f"[ann id={ann_id}] keypoints must be a flat list of length 3*K: [x0,y0,v0,...].")

        for i in range(2, len(v), 3):
            vis = int(v[i])
            if vis not in (0, 1, 2):
                raise ValueError(
                    f"[ann id={ann_id}] keypoints visibility values must be 0,1,2; got {vis} or visibility is missing from keypoints data."
                    f" keypoints format must be [x0,y0,v0,...]."
                )
            x = float(v[i-2])
            y = float(v[i-1])
            if vis == 0 and (x != 0 or y != 0):
                raise ValueError(f"[ann id={ann_id}] When visibility v=0, keypoint coordinates must be (0,0).")
            if vis > 0 and (x < 0 or y < 0):
                raise ValueError(f"[ann id={ann_id}] Visible keypoints must have non-negative coordinates.")
        return v

    @validator("segmentation", pre=True, always=True)
    def _ensure_seg_default(cls, v):
        """
        Ensure 'segmentation' defaults to `[[]]` instead of `None` for consumers
        that assume the key exists. If provided, pass through unchanged.

        Returns:
            `[[]]` when `v` is None, else the original value.
        """
        return [[]] if v is None else v

    @validator("iscrowd")
    def _check_iscrowd(cls, v, values):
        """
        Validate 'iscrowd' flag when supplied.

        Returns:
            The original value if valid, or None if not provided.

        Raises:
            ValueError: If provided and not 0 or 1.
        """
        ann_id = values.get("id", "<unknown>")  # for error messages
        if v is None:
            return v
        if not isinstance(v, int) or v not in (0, 1):
            raise ValueError(f"[ann id={ann_id}] iscrowd must be integer 0 or 1 when provided.")
        return v


def _find_dupes(seq: List[int]) -> List[int]:
    """
    Find duplicate integer elements in a sequence.

    Args:
        seq: A list of integer IDs.

    Returns:
        A list of the duplicate values (each value appears once, regardless of
        how many times it was duplicated).
    """
    # track values we've seen and collect duplicates.
    seen = set()
    dupes = set()
    for x in seq:
        if x in seen:
            dupes.add(x)
        seen.add(x)
    return list(dupes)


class CocoDataset(BaseModel):
    """
    High-level COCO-style dataset container aggregating all components.

    Attributes:
        info: Optional dataset-level metadata (name, version, etc.).
        licenses: Optional list of license entries referenced by images.
        categories: The set of categories/classes available.
        images: All image metadata entries.
        annotations: All object (and optional keypoint) annotations.

    Validation performed in the root validator:
        - Ensures unique IDs within each list (licenses, categories, images, annotations).
        - Checks that Image.license (if present) refers to a known License.id.
        - Ensures that Annotation.image_id and Annotation.category_id refer to existing
          Image.id and Category.id, respectively.
        - For categories with defined keypoints, enforces that annotations in that category:
            * Include a 'keypoints' list.
            * Have a keypoints length equal to (3 * K), where K = len(category.keypoints).
            * If 'num_keypoints' is provided, it must match the computed count of visible
              keypoints where visibility v > 0.
    """
    info: Optional[Info] = None
    licenses: Optional[List[License]] = None
    categories: List[Category]
    images: List[Image]
    annotations: List[Annotation]

    class Config:
        extra = "allow"


    @root_validator
    def _uniqueness_and_refs(cls, values):
        """
        Enforce cross-object uniqueness and reference integrity.

        Steps:
            1) Collect each section (licenses, categories, images, annotations).
            2) Ensure unique 'id' values within each section.
            3) Build lookup sets (license_ids, category_ids, image_ids).
            4) Validate that Image.license (when provided) exists in license_ids.
            5) Build a per-category expected keypoint count: len(keypoints) or None.
            6) For each annotation:
                - image_id must exist in image_ids.
                - category_id must exist in category_ids.
                - If the category defines keypoints:
                    * 'keypoints' must be present on the annotation.
                    * Its length must be exactly 3 * K.
                    * If 'num_keypoints' is present, it must equal the number of
                      visible keypoints (v > 0) computed from the annotation.

        Returns:
            The original values dict if all checks pass.

        Raises:
            ValueError: If any integrity rule is violated.
        """
        
        # extract sections, defaulting to empty lists if not available.
        licenses = values.get("licenses") or []
        categories = values.get("categories") or []
        images = values.get("images") or []
        annotations = values.get("annotations") or []

        # unique IDs within each list
        def ensure_unique(objs, label):
            """
            Helper to assert that each object list has unique 'id' fields.

            Args:
                objs: A list of Pydantic models each with an 'id' attribute.
                label: Text label used to format error messages.
            """
            ids = [o.id for o in objs]
            dupes = _find_dupes(ids)
            if dupes:
                raise ValueError(f"Duplicate {label} id(s): {sorted(dupes)}")

        # ensure category names are unique (not just ids)
        cat_names = [c.name for c in categories]
        if len(set(cat_names)) != len(cat_names):
            raise ValueError(f"Duplicate category name(s) found: {sorted([n for n in set(cat_names) if cat_names.count(n)>1])}")

        # enforce contiguous ids starting at 1: 1..len(categories)
        expected_ids = list(range(1, len(categories) + 1))
        actual_ids = sorted([c.id for c in categories])
        if actual_ids != expected_ids:
            raise ValueError(f"Category ids must be contiguous starting at 1; expected {expected_ids}, got {actual_ids}")

        ensure_unique(licenses, "license")
        ensure_unique(categories, "category")
        ensure_unique(images, "image")
        ensure_unique(annotations, "annotation")

        # ID lookup sets for foreign-key checks 
        license_ids = {l.id for l in licenses}
        category_ids = {c.id for c in categories}
        image_ids = {im.id for im in images}

        # validate image -> license references (if any)
        for im in images:
            if im.license is not None and im.license not in license_ids:
                raise ValueError(f"Image id={im.id} references unknown license id={im.license}")
        
        # ensure image file_name are non-empty strings and unique
        file_names = [im.file_name for im in images]
        if any((not isinstance(fn, str) or not fn.strip()) for fn in file_names):
            raise ValueError("All images must have a non-empty 'file_name' string.")
        dupe_fns = [fn for fn in set(file_names) if file_names.count(fn) > 1]
        if dupe_fns:
            raise ValueError(f"Duplicate image file_name(s): {sorted(dupe_fns)}")

        # build expected keypoint counts per category
        # if a category has no keypoints, expected_k is None, otherwise K=len(keypoints).
        cat_kp_len: Dict[int, Optional[int]] = {}
        for c in categories:
            cat_kp_len[c.id] = len(c.keypoints) if c.keypoints else None

        # validate annotations: annotation foreign keys and keypoints rules
        for ann in annotations:
            # image_id / category_id must exist
            if ann.image_id not in image_ids:
                raise ValueError(f"Annotation id={ann.id} references unknown image_id={ann.image_id}")
            if ann.category_id not in category_ids:
                raise ValueError(f"Annotation id={ann.id} references unknown category_id={ann.category_id}")

            # for this annotation, look up how many keypoints are expected given its category
            expected_k = cat_kp_len.get(ann.category_id)
    
            if expected_k:  # keypoints are required for this category
                if ann.keypoints is None:  # keypoints must be present
                    raise ValueError(
                        f"Annotation id={ann.id} (category_id={ann.category_id}) must include 'keypoints'."
                    )
                    
                # annotation must include keypoints with length exactly 3*K.
                triplets = len(ann.keypoints) // 3
                if triplets != expected_k:
                    raise ValueError(
                        f"Annotation id={ann.id}: keypoints length must be {expected_k*3} "
                        f"(got {len(ann.keypoints)})."
                    )

                # validate visibility counts with num_keypoints in annotation
                # if num_keypoints is given, it must equal visible count (v > 0).
                if ann.num_keypoints is not None:
                    # visibility v is every 3rd element starting at index 2: [x, y, v].
                    visible = sum(1 for i in range(2, len(ann.keypoints), 3) if int(ann.keypoints[i]) > 0)
                    if ann.num_keypoints != visible:
                        raise ValueError(
                            f"Annotation id={ann.id}: num_keypoints={ann.num_keypoints} "
                            f"but computed visible={visible}."
                        )

        return values
    

if __name__ == "__main__":
    import json
    annot_path = "/path/to/annotations/coco_train.json"
    with open(annot_path) as f:
        data = json.load(f)
    coco = CocoDataset.parse_obj(data)
    print(coco)
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, validator, root_validator

class ImageFeature(BaseModel):
    """
    Declarative descriptor for an image feature in the dataset schema.

    Attributes
    ----------
    id : Optional[Any]
        Optional identifier for the feature. Not used by validators but useful
        for downstream tooling that wants to track features by an ID.
    type: Literal["Image"]
        Discriminator that identifies this feature as an image.
    """
    id: Optional[Any] = None
    type: Literal["Image"] = Field(alias="_type") # because fields starting with an underscore are treated as private attributes by default
    
    class Config:
        allow_population_by_field_name = False  # populate via alias "_type" (default OK)


class TextFeature(BaseModel):
    """
    Declarative descriptor for a text feature in the dataset schema.

    Attributes
    ----------
    id : Optional[Any]
        Optional identifier for the feature.
    type : Literal["Text"]
        Discriminator that identifies this feature as text.
    """
    id: Optional[Any] = None
    type: Literal["Text"] = Field(alias="_type")
    
    class Config:
        allow_population_by_field_name = False


class BBoxFeature(BaseModel):
    """
    Declarative descriptor for a bounding-box feature in the dataset schema.

    Attributes
    ----------
    id : Optional[Any]
        Optional identifier for the feature.
    type : Literal["BBoxFeature"]
        Discriminator that identifies this feature as a bounding-box container.
        (Actual numeric bbox values live in annotations; this is a *feature type*
        declaration.)
    """
    id: Optional[Any] = None
    type: Literal["BBoxFeature"] = Field(alias="_type")
    
    class Config:
        allow_population_by_field_name = False


class ClassLabel(BaseModel):
    """
    Declarative descriptor for a categorical label space.

    Attributes
    ----------
    id : Optional[Any]
        Optional identifier for the feature.
    num_classes : Optional[int]
        Number of classes. If provided, should be equal to len(names).
    names : Optional[List[str]]
        Ordered list of class names. Index/position usually maps to class id.
    type : Literal["ClassLabel"]
        Discriminator for this feature type.
    """
    id: Optional[Any] = None
    num_classes: Optional[int] = None
    names: Optional[List[str]] = None
    type: Literal["ClassLabel"] = Field(alias="_type")

    class Config:
        allow_population_by_field_name = False

    
class SequenceFeature(BaseModel):
    """
    Declarative descriptor for a sequence/array feature, typically used to model
    a list of objects per image (e.g., detection/keypoint instances).

    Attributes
    ----------
    id : Optional[Any]
        Optional identifier for the feature.
    _type : Literal["Sequence"]
        Discriminator indicating this is a sequence container.
    objects_bbox : Optional[BBoxFeature]
        If present, declares that each sequence item carries a bbox field.
    objects_label : Optional[ClassLabel]
        If present, declares that each sequence item carries a class label.
    objects_keypoint : Optional["SequenceFeature"]
        If present, declares that each sequence item carries a *nested sequence*
        (e.g., per-object keypoints list). Uses a string forward reference to
        avoid circular typing.
    """
    id: Optional[Any] = None
    type: Literal["Sequence"] = Field(alias="_type")
    # for per-object attributes: allow nested fields (bbox/label/keypoints)
    objects_bbox: Optional[BBoxFeature] = None
    objects_label: Optional[ClassLabel] = None
    objects_keypoint: Optional["SequenceFeature"] = None


class Features(BaseModel):
    """
    Top-level feature declaration for the dataset.

    Attributes
    ----------
    image : Optional[ImageFeature]
        Declares the primary image feature (if the dataset is image-based).
    labels : Optional[SequenceFeature]
        Declares a sequence of per-image labels/objects (e.g., boxes, classes,
        keypoints). The internal structure of `labels` determines which fields
        exist on each object (see `SequenceFeature`).
    """
    image: Optional[ImageFeature] = None
    labels: Optional[SequenceFeature] = None
    class Config:
        extra = "allow"


class SplitInfo(BaseModel):
    """
    Summary statistics for a dataset split (e.g., train/validation/test).

    Attributes
    ----------
    name : str
        Split name (e.g., "train", "validation", "test").
    dataset_name : str
        Human-readable or programmatic dataset identifier this split belongs to.
    num_examples : Optional[int]
        Number of examples in the split (if known).
    num_bytes : Optional[int]
        Total on-disk size of the split (if known), in bytes.

    Config
    ------
    extra = "allow"
        Allows additional split metadata without failing validation.
    """
    name: str
    dataset_name: str
    num_examples: Optional[int] = None
    num_bytes: Optional[int] = None

    class Config:
        extra = "allow"


class TaskTemplate(BaseModel):
    """
    Declares the single supervised task this dataset is intended to support.

    Attributes
    ----------
    task : Literal["classification", "detection", "keypoints"]
        Task family. Drives validation expectations for `annotations`.
    labels : List[str]
        Ordered list of class names for the task. Position usually maps to id.
    num_keypoints : Optional[int]
        Required when `task == "keypoints"`. The total number of keypoints per
        instance (visible or not).
    annotations : Optional[List[str]]
        Names of the fields expected inside an *annotation record* for this task.
        For example:
          * classification → {"category_id"}
          * detection → {"category_id", "bbox"}
          * keypoints → {"category_id", "bbox", "keypoints"}

    Validators
    ----------
    _check_num_keypoints :
        Ensures `num_keypoints` is present and > 0 when `task == "keypoints"`.
    _check_annotations_nonempty :
        If `annotations` is provided, ensures it includes the minimal set of
        required fields for the declared `task`. (Extra fields are allowed.)
    """
    task: Literal["classification", "detection", "keypoints"]
    labels: List[str]
    num_keypoints: Optional[int] = None
    annotations: Optional[List[str]] = None

    @validator("num_keypoints", always=True, pre=True)
    def _check_num_keypoints(cls, v, values):
        """
        Require `num_keypoints` for keypoint tasks and ensure it is a positive integer.
        Runs even when the field is missing (`always=True`).
        """
        if values.get("task") == "keypoints":
            if not isinstance(v, int) or v <= 0:
                raise ValueError("For task='keypoints', num_keypoints must be a positive integer.")
        return v

    @validator("annotations", always=True)
    def _check_annotations_nonempty(cls, v, values):
        """
        If `annotations` is provided, enforce task-specific minimum required fields.

        The validator **does not** forbid additional fields; it only checks that
        the *required* subset for the chosen task is present.
        """
        if v is None:
            return v

        required_by_task = {
            "classification": {"category_id"},
            "detection": {"category_id", "bbox"},
            "keypoints": {"category_id", "bbox", "keypoints"},
        }
        task = values.get("task")
        if task in required_by_task:
            missing = required_by_task[task] - set(v)
            if missing:
                raise ValueError(
                    f"For task='{task}', annotations must include {sorted(required_by_task[task])}; "
                    f"missing: {sorted(missing)}"
                )
        return v

    
class DatasetInfo(BaseModel):
    """
    Container for dataset-level metadata, split summaries, and a single task template.

    Attributes
    ----------
    task_templates : List[TaskTemplate]
        A list of task templates. This schema expects *exactly one* template,
        which is enforced by the `check_min_length` validator.
    splits : Dict[str, SplitInfo]
        Mapping of split name → `SplitInfo`. A later root validator ensures
        that at least "train", "validation", and "test" are present.
    description : str
        Human-readable dataset description (optional, defaults to empty string).
    citation : str
        Citation information for the dataset (optional).
    homepage : str
        Link to the dataset homepage (optional).
    license : str
        License string (optional).
    features : Optional[Features]
        Declarative feature description (image + object structure).
    post_processed, supervised_keys, builder_name, config_name, version,
    download_size, post_processing_size, dataset_size, size_in_bytes :
        Optional auxiliary metadata fields commonly seen in TFDS-style infos.

    Validators
    ----------
    check_min_length :
        Enforces that exactly one `TaskTemplate` is provided.
    _require_three_splits :
        Root validator that checks the presence of the canonical three splits:
        "train", "validation", and "test". Additional splits may exist.

    Design
    ------
    These validators are intentionally conservative: they ensure a minimal set
    of invariants so that downstream training/evaluation code can make simple
    assumptions about what exists without defensive checks everywhere.
    """
    task_templates: List[TaskTemplate]
    splits: Dict[str, SplitInfo]
    description: str = ""
    citation: str = ""
    homepage: str = ""
    license: str = ""
    features: Optional[Features] = None
    post_processed: Optional[Any] = None
    supervised_keys: Optional[Any] = None
    builder_name: Optional[str] = None
    config_name: Optional[str] = None
    version: Optional[Dict] = None
    download_size: Optional[int] = None
    post_processing_size: Optional[int] = None
    dataset_size: Optional[int] = None
    size_in_bytes: Optional[int] = None

    @validator("task_templates")
    def check_min_length(cls, v):
        """
        Require exactly one task template.

        Rationale
        ---------
        Many pipelines assume a single supervised task per dataset/config.
        If you need multiple templates, consider multiple dataset configs.
        """
        if len(v) != 1:
            raise ValueError("task_templates must contain exactly one element")
        return v

    @root_validator
    def _require_three_splits(cls, values):
        """
        Ensure canonical split keys exist: "train", "validation", and "test".

        Behavior
        --------
        - Fails fast if any of the three are missing.
        - Permits extra splits (e.g., 'train_small', 'val2017', 'ood_test').

        Notes
        -----
        The debug `print` can be useful while wiring up configs and can be
        removed in production if verbose logs are undesirable.
        """
        splits = values.get("splits")
        
        if not isinstance(splits, dict) or not splits:
            raise ValueError("`splits` must be a non-empty mapping with keys: 'train', 'validation', 'test'.")
    
        required = ("train", "validation", "test")
        missing = [k for k in required if k not in splits]
        if missing:
            raise ValueError(f"splits must include {required}; missing: {missing}")

        return values


class DatasetInfos(BaseModel):
    """
    Container for the top-level dataset_infos.json file.

    This model treats the JSON as a mapping:
        {
            "<dataset_name>": <DatasetInfo>,
            "<dataset_name2>": <DatasetInfo>,
            ...
        }
    """
    __root__: Dict[str, DatasetInfo]

    class Config:
        extra = "forbid"
        
    # make it dict-like
    def __getitem__(self, key: str) -> DatasetInfo:
        return self.__root__[key]

    def __iter__(self):
        return iter(self.__root__)

    def __len__(self) -> int:
        return len(self.__root__)

    def keys(self):
        return self.__root__.keys()

    def values(self):
        return self.__root__.values()

    def items(self):
        return self.__root__.items()

    def get(self, key: str, default=None):
        return self.__root__.get(key, default)


if __name__ == "__main__":
    import json
    dataset_infos_path = "/path/to/dataset_infos.json"
    with open(dataset_infos_path) as f:
        data = json.load(f)
    dataset_info = DatasetInfos.parse_obj(data)
    print(dataset_info)

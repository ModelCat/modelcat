from checksumdir import dirhash


def hash_dataset(dataset_root: str, threads: int = 8, algorithm="sha256"):
    return dirhash(
        dataset_root, hashfunc=algorithm, excluded_files=["dataset_validator_log.txt"], ignore_hidden=True,
    )

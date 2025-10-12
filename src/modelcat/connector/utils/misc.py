import os
import os.path as osp
import modelcat.connector as connector


def _get_dataset_path() -> str:
    datasets_folder = 'sample_datasets'

    # first try where package is installed
    # this will work for local call of unittests
    ds_path = osp.abspath(osp.join(osp.dirname(connector.__file__), "..", "..", "..", datasets_folder))
    if osp.exists(ds_path):
        print(ds_path)
        return ds_path

    # otherwise check current directory (this will work for tox)

    ds_path = osp.abspath(osp.join(os.getcwd(), datasets_folder))
    if osp.exists(ds_path):
        print(ds_path)
        return ds_path

    raise FileExistsError(f'`{datasets_folder}` cannot be located')
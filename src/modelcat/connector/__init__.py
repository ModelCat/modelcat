from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("modelcat")
except PackageNotFoundError:
    __version__ = "dev"

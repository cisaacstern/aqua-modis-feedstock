import datetime as dt
import os

import aiohttp
import apache_beam as beam
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.transforms import (
    Indexed,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    T,
)

dates = pd.date_range("2002-07-04", "2002-07-11", freq="8D")
variables = ["CHL.chlor_a", "IOP.bbp_443", "SST.sst"]


def make_modis_url(time: pd.Timestamp, var: str) -> str:
    fmt = "%Y%m%d"
    end = time + dt.timedelta(days=7)
    return (
        # "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/"
        "./example-data/"  # for local testing, with data pre-fetched via wget
        f"AQUA_MODIS.{time.strftime(fmt)}_{end.strftime(fmt)}.L3m.8D.{var}.4km.nc"
    )


concat = ConcatDim("time", keys=dates)
merge = MergeDim("var", keys=variables)
pattern = FilePattern(make_modis_url, concat, merge)

username, password = os.environ["EARTHDATA_USERNAME"], os.environ["EARTHDATA_PASSWORD"]
client_kwargs = {
    "auth": aiohttp.BasicAuth(username, password),
    "trust_env": True,
}

class Preprocess(beam.PTransform):
    """Preprocessor transform."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        import numpy as np

        index, ds = item
        ds = ds.drop("palette")
        time = np.datetime64(ds.attrs["time_coverage_start"].replace("Z", ""))
        ds = ds.expand_dims(time=np.array([time]))
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)


transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(
        # open_kwargs={"client_kwargs": client_kwargs},
        # max_concurrency=1,
    )
    | OpenWithXarray()
    | Preprocess()
    | StoreToZarr(
        store_name="modis.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 1, "lat": 4320 / 2, "lon": 8640 / 2},
    )
)

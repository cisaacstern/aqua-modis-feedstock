import datetime as dt
import itertools
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
 
def make_dates(freq="8D"):
    """Create the list of dates of available data."""
    start, end = "01-01", "12-27"  # annual start and end days
    # first year starts in july
    _2002 = pd.date_range("2002-07-04", f"2002-{end}", freq=freq).to_list()
    # no missing data for these years. first create a nested list, then flatten it.
    _2003_2021_nested = [
        pd.date_range(f"{yr}-{start}", f"{yr}-{end}", freq=freq).to_list()
        for yr in range(2003, 2022)
    ]
    _2003_2021 = list(itertools.chain.from_iterable(_2003_2021_nested))
    _2022 = (  # special case this year beacuse there's one day ("2022-04-07") of missing data
        pd.date_range(f"2022-{start}", f"2022-{end}", freq=freq).drop(pd.Timestamp("2022-04-07"))
    ).to_list()
    # final year is a special case because it ends in july
    _2023 = pd.date_range(f"2023-{start}", f"2023-07-27", freq=freq).to_list()
    # now flatten everything 
    return list(itertools.chain.from_iterable([_2002, _2003_2021, _2022, _2023]))


dates = make_dates()
variables = ["CHL.chlor_a", "IOP.bbp_443", "SST.sst"]


def make_modis_url(time: pd.Timestamp, var: str) -> str:
    fmt = "%Y%m%d"
    end = time + dt.timedelta(days=7)
    return (
        "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/"
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
        open_kwargs={"block_size": 0, "client_kwargs": client_kwargs},
        max_concurrency=10,
    )
    | OpenWithXarray()
    | Preprocess()
    | StoreToZarr(
        store_name="aqua-modis.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 1, "lat": int(4320 / 2), "lon": int(8640 / 2)},
    )
)

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
    yrs = {  # start with a dict of dates as if every year was complete...
        yr: pd.date_range(f"{yr}-01-01", f"{yr}-12-27", freq=freq) for yr in range(2002, 2024)
    }
    # ...but we need to make some edits due to missing data
    yrs[2002] = yrs[2002][slice(*yrs[2002].slice_locs("2002-07-04", "2002-12-27"))]
    yrs[2022] = yrs[2022].drop("2022-04-07")  # missing for `sst`, but not `bbp_403` + `chlor_a`
    yrs[2023] = yrs[2023][slice(*yrs[2023].slice_locs("2023-01-01", "2023-07-20"))]
    # now flatten everything to a single list
    return list(itertools.chain.from_iterable(yrs.values()))


dates = make_dates()
variables = ["CHL.chlor_a", "IOP.bbp_443", "SST.sst"]


def make_modis_url(time: pd.Timestamp, var: str) -> str:
    fmt = "%Y%m%d"
    end = (
        # typically the end timestamp is 7 days ahead...
        time + dt.timedelta(days=7)
        # unless this is dec26 or dec27, then end is dec31
        if not time.strftime("%m-%d") in ("12-26", "12-27")
        else pd.Timestamp(year=time.year, month=12, day=31)
    )
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
        target_root=".",  # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/587
    )
)

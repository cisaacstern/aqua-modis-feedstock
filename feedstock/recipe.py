import datetime as dt
import os

import aiohttp
import apache_beam as beam
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr

dates = pd.date_range("2002-07-04", "2002-07-19", freq="8D")
variables = ["CHL.chlor_a", "IOP.bbp_443", "SST.sst"]


def make_modis_url(date: pd.Timestamp, var: str) -> str:
    fmt = "%Y%m%d"
    end = date + dt.timedelta(days=7)
    return (
        "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS."
        f"{date.strftime(fmt)}_{end.strftime(fmt)}.L3m.8D.{var}.4km.nc"
    )


concat = ConcatDim("date", keys=dates)
merge = MergeDim("var", keys=variables)
pattern = FilePattern(make_modis_url, concat, merge)

username, password = os.environ["EARTHDATA_USERNAME"], os.environ["EARTHDATA_PASSWORD"]
client_kwargs = {
    "auth": aiohttp.BasicAuth(username, password),
    "trust_env": True,
}

transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(
        open_kwargs={"client_kwargs": client_kwargs},
        max_concurrency=1,
    )
    | OpenWithXarray()
    | StoreToZarr(
        store_name="modis.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)

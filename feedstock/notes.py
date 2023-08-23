# new:
# - synthesis of multiple sources -> one zarr output 
# - at different spatial resolution
# - and different temporal resolution
# - and sources are not all in same format

import xarray as xr

# mur-sst
url = "https://mur-sst.s3.us-west-2.amazonaws.com/zarr-v1"
ds = xr.open_dataset(url, engine="zarr", chunks={})
print(ds.time.values[0], ds.time.values[-1])
# (numpy.datetime64('2002-06-01T09:00:00.000000000'),
#  numpy.datetime64('2020-01-20T09:00:00.000000000'))

# ocean color
# https://oceancolor.gsfc.nasa.gov/data/download_methods/
# wget --user=USERNAME --ask-password --auth-no-challenge=on https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS.20020704_20020711.L3m.8D.CHL.chlor_a.4km.nc
url = "https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS.20020704_20020711.L3m.8D.CHL.chlor_a.4km.nc"
local = "AQUA_MODIS.20020704_20020711.L3m.8D.CHL.chlor_a.4km.nc.1"
ds = xr.open_dataset(local)
# NOTE: no `time` dimension, time range in attributes
ds.attrs["time_coverage_start"], ds.attrs["time_coverage_end"]
# ('2002-07-04T00:55:01.000Z', '2002-07-12T02:19:59.000Z')

# backscattering
# same access pattern as color
# on https://oceandata.sci.gsfc.nasa.gov/api/file_search/, use "MODIS-Aqua IOP Level 3-Mapped"
# e.g. --> AQUA_MODIS.20020704_20020711.L3m.8D.IOP.bbp_443.4km.nc
# NOTE: **bbp_443.4km**

# salinity
# landing page: https://podaac.jpl.nasa.gov/dataset/OISSS_L4_multimission_7day_v2
# example url: https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v2/OISSS_L4_multimission_global_7d_v2.0_2011-08-28.nc
# same command wget command as ocean color works, e.g.
# wget --user=cisaacstern --ask-password --auth-no-challenge=on https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v2/OISSS_L4_multimission_global_7d_v2.0_2011-08-28.nc
# --> OISSS_L4_multimission_global_7d_v2.0_2011-08-28.nc
# 4 day resolution, starts on **2011-08-28**

# Overall time resolution
# 2011 (salinity start) - 2020 (mur sst end)

# new idea:
# everything except salinity from modis
# salinity is least-well correlated, and in-situ salinity compares poorly with satellite (sometimes)

# modis sst
# https://oceandata.sci.gsfc.nasa.gov/ob/getfile/AQUA_MODIS.20020728_20020804.L3m.8D.SST.sst.4km.nc

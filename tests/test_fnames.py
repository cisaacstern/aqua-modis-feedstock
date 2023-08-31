import os
import sys
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest


@dataclass
class RecipeAttrs:
    dates: list
    make_modis_url: callable
    variables: list


@pytest.fixture(scope="session")
def recipe_attrs() -> RecipeAttrs:
    # 'feedstock' is not actually an installed package, so make it discoverable here.
    # (perhaps there's a better or more standard way to do this  described in:
    # https://docs.pytest.org/en/7.1.x/explanation/pythonpath.html ?)
    feedstock = (Path(__file__).parent.parent / "feedstock").absolute().as_posix()
    sys.path.append(feedstock)
    with mock.patch.dict(os.environ, {"EARTHDATA_USERNAME": "FOO", "EARTHDATA_PASSWORD": "BAR"}):
        from recipe import dates, make_modis_url, variables  # type: ignore
        yield RecipeAttrs(dates, make_modis_url, variables)
        # teardown
        sys.path.remove(feedstock)


@pytest.fixture
def expected(recipe_attrs: RecipeAttrs):
    """The expected fnames."""

    # load all filenames text files
    fnames = []
    for p in Path("resources/filenames").iterdir():
        with p.open() as f:
            fnames += f.read().splitlines()

    # filter filenames to only 4km data for the selected variables
    expected = [
        f for f in fnames if "4km" in f and any([f".{v}" in f for v in recipe_attrs.variables])
    ]
    # we've found that the following date is missing from sst *only* (not other variables)
    missing = "20220407"
    # first of all, confirm that this is indeed the case
    assert not any([(missing in f and "sst" in f) for f in expected])  # missing in sst
    assert any([(missing in f and "chlor_a" in f) for f in expected])  # present in chlor_a
    assert any([(missing in f and "bbp_443" in f) for f in expected])  # present in bbp_443
    # now drop it from all variables, because we're not currently using it in the recipe
    expected = [e for e in expected if "20220407" not in e]
    expected.sort()
    return expected


@pytest.fixture
def generated(recipe_attrs: RecipeAttrs):
    """Generate fnames using our recipe logic.
    Note that the `expected` list is *just* filenames (not full urls), so we parse accordingly.
    """
    generated = [
        recipe_attrs.make_modis_url(d, var).split("getfile/")[-1]
        for d in recipe_attrs.dates
        for var in recipe_attrs.variables]
    generated.sort()
    return generated


@pytest.fixture
def diff(expected: list, generated: list) -> list[dict]:
    """Two-way diff of the fname lists."""
    expected_but_not_generated = list(set(expected) - set(generated))
    generated_but_not_expected = list(set(generated) - set(expected))
    return expected_but_not_generated, generated_but_not_expected


def test_fnames(diff: tuple[list, list]):
    """Check that there is no difference between expected and generated."""

    for d in diff:
        # if there is a diff, pytest will print it for us in the AssertionError raised here, e.g.:
        #    ```
        #    >   assert len(d) == 0
        #    E   AssertionError: assert 1 == 0
        #    E    +  where 1 = len(['AQUA_MODIS.20220415_20220422.L3m.8D.CHL.chlor_a.4km.nc'])
        #    ```
        assert len(d) == 0

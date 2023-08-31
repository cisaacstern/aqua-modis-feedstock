import sys
from pathlib import Path

import pytest

# 'feedstock' is not actually an installed package, so make it discoverable here
sys.path.append((Path(__file__).parent.parent / "feedstock").absolute().as_posix())
from recipe import dates, make_modis_url, variables  # type: ignore


@pytest.fixture
def expected():
    """The expected fnames."""

    # load all filenames text files
    fnames = []
    for p in Path("resources/filenames").iterdir():
        with p.open() as f:
            fnames += f.read().splitlines()

    # filter filenames to only 4km data for the selected variables
    expected = [f for f in fnames if "4km" in f and any([f".{v}" in f for v in variables])]
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
def generated():
    """Generate fnames using our recipe logic.
    Note that the `expected` list is *just* filenames (not full urls), so we parse accordingly.
    """
    generated = [make_modis_url(d, var).split("getfile/")[-1] for d in dates for var in variables]
    generated.sort()
    return generated


@pytest.fixture
def diff(expected: list, generated: list) -> list[dict]:
    """Two-way diff of the fname lists."""
    expected_but_not_generated = list(set(expected) - set(generated))
    generated_but_not_expected = list(set(generated) - set(expected))

    return [
        {"exp": exp, "gen": gen}
        for exp, gen
        in zip(expected_but_not_generated, generated_but_not_expected)
    ]


def test_fnames(diff: list[dict]):
    """Check that there is no difference between expected and generated."""

    # if there is a difference, print it for reference
    for d in diff:
        for k, v in d.items():
            print(k, v)
        print("-")
    # but there shouldn't be one
    assert not diff

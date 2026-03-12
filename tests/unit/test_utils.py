"""Tests for shared utility functions."""

from __future__ import annotations

from impact.common.utils import strip_source_prefixes


class TestStripSourcePrefixes:
    """Test that source prefixes are stripped correctly without corrupting package names."""

    def test_strips_source_prefix(self):
        assert strip_source_prefixes("facility_main.amount", {"facility_main"}) == "amount"

    def test_strips_multiple_prefixes(self):
        result = strip_source_prefixes(
            "facility_main.amount - collateral.value",
            {"facility_main", "collateral"},
        )
        assert result == "amount - value"

    def test_longest_first(self):
        """Longer source names are stripped first to avoid partial matches."""
        result = strip_source_prefixes(
            "other_facility.amount", {"facility", "other_facility"}
        )
        assert result == "amount"

    def test_preserves_pd_prefix(self):
        """pd.isna() must not be corrupted even if 'pd' is a source name."""
        result = strip_source_prefixes("pd.isna(amount)", {"pd", "main"})
        assert result == "pd.isna(amount)"

    def test_preserves_np_prefix(self):
        """np.log() must not be corrupted even if 'np' is a source name."""
        result = strip_source_prefixes("np.log(value)", {"np", "main"})
        assert result == "np.log(value)"

    def test_preserves_pandas_prefix(self):
        """pandas.isna() must not be corrupted even if 'pandas' is a source name."""
        result = strip_source_prefixes("pandas.isna(amount)", {"pandas"})
        assert result == "pandas.isna(amount)"

    def test_strips_source_but_not_package_in_same_expr(self):
        """Source prefix stripped, package prefix preserved in the same expression."""
        result = strip_source_prefixes(
            "facility.amount * pd.isna(facility.rate)",
            {"facility"},
        )
        assert result == "amount * pd.isna(rate)"

    def test_no_strip_when_preceded_by_word_char(self):
        """Source name preceded by a word character is not stripped."""
        # e.g. 'my_pd.col' where source is 'pd' — 'pd.' is preceded by '_'
        result = strip_source_prefixes("my_pd.col", {"pd"})
        assert result == "my_pd.col"

    def test_pass_through_no_prefix(self):
        assert strip_source_prefixes("amount", {"facility_main"}) == "amount"

    def test_empty_sources(self):
        assert strip_source_prefixes("facility_main.amount", set()) == "facility_main.amount"

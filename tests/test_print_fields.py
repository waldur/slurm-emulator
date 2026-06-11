"""Unit tests for the shared print_fields renderer.

Expected byte shapes come from real Slurm's src/common/print_fields.c:
every column (including the last) is followed by a space, the dash row
matches column widths, and over-wide string cells truncate to
``value[:width-1] + '+'``.
"""

import pytest

from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    extract_output_flags,
    parse_format_spec,
    render_header,
    render_row,
    render_table,
    resolve_format,
)

ACCOUNT = FieldSpec("Account", 10)
DESCR = FieldSpec("Descr", 20, header="Descr")
JOBID = FieldSpec("JobID", -12)
NUM = FieldSpec("AllocCPUS", 10, truncate=False)
REGISTRY = [ACCOUNT, DESCR, JOBID, NUM]


class TestRenderHeader:
    def test_default_mode_name_and_dash_rows(self):
        lines = render_header([ACCOUNT, DESCR], OutputMode())
        assert lines == [
            "   Account                Descr ",
            "---------- -------------------- ",
        ]

    def test_left_aligned_header(self):
        lines = render_header([JOBID], OutputMode())
        assert lines == ["JobID        ", "------------ "]

    def test_parsable_p_trailing_pipe(self):
        lines = render_header([ACCOUNT, DESCR], OutputMode(parsable="p"))
        assert lines == ["Account|Descr|"]

    def test_parsable2_no_trailing_pipe(self):
        lines = render_header([ACCOUNT, DESCR], OutputMode(parsable="P"))
        assert lines == ["Account|Descr"]

    def test_noheader(self):
        assert render_header([ACCOUNT], OutputMode(noheader=True)) == []

    def test_header_name_clipped_without_plus(self):
        wide_name = FieldSpec("VeryLongFieldName", 8)
        lines = render_header([wide_name], OutputMode())
        assert lines[0] == "VeryLong "


class TestRenderRow:
    def test_right_align_padding(self):
        assert render_row(["acct"], [ACCOUNT], OutputMode()) == "      acct "

    def test_left_align_padding(self):
        assert render_row(["123"], [JOBID], OutputMode()) == "123          "

    def test_truncation_with_plus(self):
        out = render_row(["abcdefghijkl"], [ACCOUNT], OutputMode())
        assert out == "abcdefghi+ "

    def test_numeric_field_never_truncates(self):
        out = render_row(["123456789012"], [NUM], OutputMode())
        assert out == "123456789012 "

    def test_parsable_does_not_truncate(self):
        out = render_row(["abcdefghijkl"], [ACCOUNT], OutputMode(parsable="P"))
        assert out == "abcdefghijkl"

    def test_parsable_modes(self):
        assert render_row(["a", "b"], [ACCOUNT, DESCR], OutputMode(parsable="p")) == "a|b|"
        assert render_row(["a", "b"], [ACCOUNT, DESCR], OutputMode(parsable="P")) == "a|b"


class TestRenderTable:
    def test_dict_rows_missing_keys_blank(self):
        out = render_table(
            [ACCOUNT, DESCR],
            [{"Account": "root"}],
            OutputMode(parsable="P", noheader=True),
        )
        assert out == "root|"

    def test_list_rows(self):
        out = render_table([ACCOUNT], [["a"], ["b"]], OutputMode(parsable="P", noheader=True))
        assert out == "a\nb"


class TestResolveFormat:
    def test_prefix_match_case_insensitive(self):
        fields = resolve_format(parse_format_spec("acc,desc"), REGISTRY)
        assert [f.name for f in fields] == ["Account", "Descr"]

    def test_width_override(self):
        fields = resolve_format(parse_format_spec("Account%5"), REGISTRY)
        assert fields[0].width == 5

    def test_negative_width_override_flips_alignment(self):
        fields = resolve_format(parse_format_spec("Account%-15"), REGISTRY)
        assert fields[0].width == -15
        assert not fields[0].right_align

    def test_unknown_field_raises(self):
        with pytest.raises(UnknownFieldError) as exc:
            resolve_format(parse_format_spec("Bogus"), REGISTRY)
        assert exc.value.token == "Bogus"  # noqa: S105 — field name, not a password

    def test_min_prefix_enforced(self):
        registry = [FieldSpec("Cluster", 10, min_prefix=2)]
        with pytest.raises(UnknownFieldError):
            resolve_format([("c", None)], registry)
        assert resolve_format([("cl", None)], registry)[0].name == "Cluster"


class TestExtractOutputFlags:
    def test_long_flags(self):
        mode, immediate, rest = extract_output_flags(
            ["list", "--noheader", "account", "--parsable2"], shorts="npPi"
        )
        assert mode.noheader
        assert mode.parsable == "P"
        assert not immediate
        assert rest == ["list", "account"]

    def test_combined_short_cluster(self):
        mode, _, rest = extract_output_flags(["-nP", "show", "account"])
        assert mode.noheader
        assert mode.parsable == "P"
        assert rest == ["show", "account"]

    def test_immediate_only_when_enabled(self):
        _, immediate, rest = extract_output_flags(["-i", "add"], shorts="npPi")
        assert immediate
        assert rest == ["add"]
        _, immediate, rest = extract_output_flags(["-i", "add"], shorts="npP")
        assert not immediate
        assert rest == ["-i", "add"]

    def test_unrelated_args_untouched(self):
        _, _, rest = extract_output_flags(["-S", "2024-01-01", "format=name"])
        assert rest == ["-S", "2024-01-01", "format=name"]

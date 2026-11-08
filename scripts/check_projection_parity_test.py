#!/usr/bin/env python3
"""Selftest of check_projection_parity.py: a dropped field, an extra field and a changed optional are red in the generated types and in the DTO of a bridge mapping row, equal shapes are green."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_projection_parity as projection  # noqa: E402

CORE = """
#[derive(uniffi::Record)]
pub struct Sample {
    pub id: String,
    pub local_url: Option<String>,
}

#[derive(uniffi::Enum)]
pub enum Outcome {
    Done,
    Failed { code: u16, detail: Option<String> },
}

#[derive(uniffi::Record)]
pub struct WireDto {
    pub kind: String,
    pub session_code: Option<u8>,
}
"""
SWIFT = """public struct Sample: Equatable {
    public var id: String
    public var localUrl: String?
}

public enum Outcome: Equatable {
    case done
    case failed(code: UInt16, detail: String?
    )
}

public struct WireDto: Equatable {
    public var kind: String
    public var sessionCode: UInt8?
}
"""
KOTLIN = """data class Sample (
    var `id`: kotlin.String
    ,
    var `localUrl`: kotlin.String? = null
)

sealed class Outcome {
    object Done : Outcome()
    data class Failed(
        val `code`: kotlin.UShort,
        val `detail`: kotlin.String?) : Outcome()
}

data class WireDto (
    var `kind`: kotlin.String
    ,
    var `sessionCode`: kotlin.UByte?
)
"""
DTO_SWIFT = "public struct Dto: Codable {\n  public let kind: String\n  public let sessionCode: UInt8?\n}\n"
DTO_KOTLIN = "data class Dto(\n    val kind: String,\n    @SerialName(\"session_code\") val sessionCode: UByte?,\n)\n"
BRIDGE = "| WireDto.kind, session_code | Dto.kind, sessionCode | те же поля | verbatim |\n"


class ProjectionParity(unittest.TestCase):
    def generated(self, core=CORE, swift=SWIFT, kotlin=KOTLIN, skips=()):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "core.rs").write_text(core)
            shapes = projection.core_shape(Path(directory))
        return projection.compare(shapes, projection.swift_shapes(swift), skips, "swift") + projection.compare(shapes, projection.kotlin_shapes(kotlin), skips, "kotlin")

    def dto(self, swift=DTO_SWIFT, kotlin=DTO_KOTLIN):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "core.rs").write_text(CORE)
            (Path(directory) / "bridge.md").write_text(BRIDGE)
            rows = projection.mapping_rows(Path(directory) / "bridge.md")
            return projection.compare_mappings(rows, projection.core_shape(Path(directory)), swift, kotlin, ())

    def test_equal_shapes_are_green(self):
        self.assertEqual(self.generated(), [])
        self.assertEqual(self.dto(), [])

    def test_a_dropped_field_is_red_on_the_platform_that_lacks_it(self):
        swift = SWIFT.replace("    public var localUrl: String?\n", "")
        self.assertEqual(self.generated(swift=swift), ["swift: Sample.localurl of the core has no projection field"])

    def test_an_extra_field_is_red(self):
        kotlin = KOTLIN.replace("val `detail`: kotlin.String?)", "val `detail`: kotlin.String?, val `extra`: kotlin.Int)")
        self.assertEqual(self.generated(kotlin=kotlin), ["kotlin: Outcome.failed.extra of the projection is not in the core"])

    def test_a_changed_optional_is_red(self):
        swift = SWIFT.replace("detail: String?", "detail: String")
        self.assertEqual(self.generated(swift=swift), ["swift: Outcome.failed.detail optional is True in the core and False in the projection"])

    def test_a_skip_row_with_a_reason_clears_one_field(self):
        swift = SWIFT.replace("    public var localUrl: String?\n", "")
        self.assertEqual(self.generated(swift=swift, skips={("Sample", "local_url", "swift")}), [])

    def test_a_dto_without_a_mapped_property_is_red(self):
        self.assertEqual(self.dto(swift=DTO_SWIFT.replace("  public let sessionCode: UInt8?\n", "")), ["swift: WireDto.sessioncode of the core has no property in Dto"])

    def test_a_dto_with_an_extra_property_is_red(self):
        self.assertEqual(self.dto(kotlin=DTO_KOTLIN.replace("val kind: String,", "val kind: String,\n    val extra: Int,")), ["kotlin: Dto.extra is not a field of WireDto"])

    def test_a_dto_with_a_changed_optional_is_red(self):
        self.assertEqual(self.dto(kotlin=DTO_KOTLIN.replace("UByte?", "UByte")), ["kotlin: WireDto.sessioncode optional is True in the core and False in Dto"])


if __name__ == "__main__":
    unittest.main()

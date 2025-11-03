#!/usr/bin/env python3
"""
Generate BinaryView and Utf8View test data using PyArrow.

This script creates Arrow IPC files containing BinaryView and Utf8View columns
to demonstrate cross-implementation compatibility between PyArrow and arrow-js.
"""

import pyarrow as pa
import pyarrow.ipc as ipc

def generate_binaryview_test_data():
    """Generate test data with BinaryView type."""
    # Create test data with various sizes to test both inline and out-of-line storage
    # Inline storage: <= 12 bytes
    # Out-of-line storage: > 12 bytes

    binary_data = [
        b"",                    # Empty
        b"short",               # 5 bytes - inline
        b"twelve_bytes",        # 12 bytes - inline (at boundary)
        b"this is longer than twelve bytes",  # 34 bytes - out-of-line
        b"x" * 100,            # 100 bytes - out-of-line
        b"another short",       # 13 bytes - just over inline boundary
        None,                   # Null value
        b"final value",         # 11 bytes - inline
    ]

    schema = pa.schema([
        pa.field("binary_view_col", pa.binary_view())
    ])

    arrays = [pa.array(binary_data, type=pa.binary_view())]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    return pa.Table.from_batches([batch])


def generate_utf8view_test_data():
    """Generate test data with Utf8View type."""
    # Create test data with various sizes

    string_data = [
        "",                     # Empty string
        "hello",                # 5 bytes - inline
        "twelve chars",         # 12 bytes - inline (at boundary)
        "this is a longer string that exceeds twelve bytes",  # Out-of-line
        "🚀" * 20,             # Unicode with emoji - out-of-line
        "just over 12",         # 13 bytes - just over boundary
        None,                   # Null value
        "final test",           # 10 bytes - inline
    ]

    schema = pa.schema([
        pa.field("utf8_view_col", pa.string_view())
    ])

    arrays = [pa.array(string_data, type=pa.string_view())]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    return pa.Table.from_batches([batch])


def generate_combined_test_data():
    """Generate test data with both BinaryView and Utf8View columns."""

    binary_data = [
        b"binary1",
        b"this binary data is longer than twelve bytes",
        None,
        b"bin4",
    ]

    string_data = [
        "string1",
        "this string is also longer than twelve bytes",
        None,
        "str4",
    ]

    schema = pa.schema([
        pa.field("binary_view_col", pa.binary_view()),
        pa.field("utf8_view_col", pa.string_view()),
    ])

    arrays = [
        pa.array(binary_data, type=pa.binary_view()),
        pa.array(string_data, type=pa.string_view())
    ]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    return pa.Table.from_batches([batch])


def main():
    """Generate all test files."""

    print("Generating BinaryView test data...")
    binaryview_table = generate_binaryview_test_data()
    with pa.OSFile('integration_tests/binaryview_utf8view/binaryview_test.arrow', 'wb') as f:
        with ipc.new_file(f, binaryview_table.schema) as writer:
            writer.write_table(binaryview_table)
    print(f"✓ Created binaryview_test.arrow ({len(binaryview_table)} rows)")

    print("\nGenerating Utf8View test data...")
    utf8view_table = generate_utf8view_test_data()
    with pa.OSFile('integration_tests/binaryview_utf8view/utf8view_test.arrow', 'wb') as f:
        with ipc.new_file(f, utf8view_table.schema) as writer:
            writer.write_table(utf8view_table)
    print(f"✓ Created utf8view_test.arrow ({len(utf8view_table)} rows)")

    print("\nGenerating combined test data...")
    combined_table = generate_combined_test_data()
    with pa.OSFile('integration_tests/binaryview_utf8view/combined_test.arrow', 'wb') as f:
        with ipc.new_file(f, combined_table.schema) as writer:
            writer.write_table(combined_table)
    print(f"✓ Created combined_test.arrow ({len(combined_table)} rows)")

    print("\n" + "="*60)
    print("Test data generation complete!")
    print("="*60)
    print("\nGenerated files:")
    print("  - binaryview_test.arrow: BinaryView column with 8 rows")
    print("  - utf8view_test.arrow: Utf8View column with 8 rows")
    print("  - combined_test.arrow: Both column types with 4 rows")
    print("\nThese files test:")
    print("  ✓ Inline data (≤12 bytes)")
    print("  ✓ Out-of-line data (>12 bytes)")
    print("  ✓ Null values")
    print("  ✓ Empty values")
    print("  ✓ Boundary conditions")
    print("  ✓ Unicode/emoji support")


if __name__ == "__main__":
    main()

"""DataTypes."""
import pyarrow as pa


class DataType:
    """A Class that is used to convert dataTypes from different libraries."""

    def __init__(self, udf_type: str, struct_lib_type: str, pyarrow_type: pa.DataType) -> None:
        """Initalise a DataType.

        Args:
            udf_type (str): Type in UDF Files
            struct_lib_type (str): Type in the struct Library
            pyarrow_type (pa.DataType): Type in the pyarrow Library
        """
        self._UDFType = udf_type
        self._structLibType = struct_lib_type
        self.pyarrowType = pyarrow_type

    def get_udf_type(self) -> str:
        """Get the UDF Type of the DataType Object.

        Returns:
            str: The string of how this DataType is noted in a UDF File
        """
        return self._UDFType

    def get_structlib_type(self) -> str:
        """Get the struct library Type of the DataType Object.

        Returns:
            str: The string of how this DataType is noted in the struct library
        """
        return self._structLibType

    def get_pyarrow_type(self) -> pa.DataType:
        """Get the struct pyarrow Type of the DataType Object.

        Returns:
            pa.DataType: The pyarrow type of this DataType
        """
        return self.pyarrowType


data_types = []
data_types.append(DataType("s8", "b", pa.int8()))
data_types.append(DataType("u8", "B", pa.uint8()))
data_types.append(DataType("s16", "h", pa.int16()))
data_types.append(DataType("u16", "H", pa.uint16()))
data_types.append(DataType("s32", "i", pa.int32()))
data_types.append(DataType("u24", "I", pa.uint32()))
data_types.append(DataType("u32", "I", pa.uint32()))
data_types.append(DataType("s64", "q", pa.int64()))
data_types.append(DataType("u64", "Q", pa.uint64()))
data_types.append(DataType("f", "f", pa.float32()))
data_types.append(DataType("d", "d", pa.float64()))
data_types.append(DataType("s", "s", pa.string()))
data_types.append(DataType("st", "s", pa.string()))
# data_types.append(DataType("", "?", pa.bool_()))
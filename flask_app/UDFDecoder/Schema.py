"""Schema."""
from .DataTypes import data_types


class Schema:
    """Class that saves the data from one axis of one sensor from a UDF-File."""

    def __init__(self, index: int, name: str, size_in_bytes: int, data_type: str, axis_name: str, scaling_factor: float):
        """Initialise a Object of the Schema class.

        Args:
            index (int): Index of the schema in the UDF File
            name (str): Name of the schema in the UDF File
            size_in_bytes (int): Size in Bytes of the Schema in the UDF File
            data_type (str): DataType of the Schema in the UDF File
            axis_name (str): Name of the axis of the schema in the UDF File
            scaling_factor (float): Scaling factor of the in the UDF File
        """
        self._index = index
        self._name = name.strip()
        self._sizeInBytes = size_in_bytes
        self._dataType = data_type
        self._axisName = axis_name.strip()
        self._scalingFactor = scaling_factor
        self._values = []
        self._timeStampIndices = []

    def add_value(self, value) -> None:
        """Add one value to the objects list of values.

        Args:
            value (any): Value to add to the list
        """
        self._values.append(value)

    def add_timestamp_index(self, time_stamp_index: int) -> None:
        """Add one index of one timestamp to the list of the indices of timestamps.

        Args:
            time_stamp_index (int): Index to add to the list
        """
        self._timeStampIndices.append(time_stamp_index)

    def get_index(self) -> int:
        """Get the index of the schema.

        Returns:
            int: the index
        """
        return self._index

    def get_values(self) -> list:
        """Get the list of the values.

        Returns:
            list: the list of values
        """
        return self._values

    def get_data_type(self) -> str:
        """Get the dataType of the Schema.

        Returns:
            str: the dataType of the schema
        """
        return self._dataType

    def get_name(self) -> str:
        """Get the name of the schema.

        Returns:
            str: the name of the schema
        """
        return self._name

    def get_size_in_bytes(self) -> int:
        """Get the size of any schema value in byte.

        Returns:
            int: the size in byte.
        """
        return self._sizeInBytes

    def get_axis_name(self) -> str:
        """Get the name of the axis of the schema.

        Returns:
            str: the name of the axis
        """
        return self._axisName

    def get_scaling_factor(self) -> float:
        """Get the scaling factor of the schema.

        Returns:
            float: the scaling factor
        """
        return self._scalingFactor

    def get_timestamp_indices(self) -> list:
        """Get the Indices of the timestamps.

        Returns:
            list: the list of timestamp indices
        """
        return self._timeStampIndices

    def get_amount_of_values(self) -> int:
        """Get the amount of the values saved.

        Uses the len() function on the list of values to get the amount.

        Returns:
            int: _description_
        """
        return len(self._values)

    def get_datatype_for_struct_lib(self) -> str:
        """Get the correlating datatype of the struct library to the schema's UDF datatype.

        Returns:
            str: struct datatype
        """
        for data_type in data_types:
            if data_type.get_udf_type() == self._dataType:
                return data_type.get_structlib_type()

    def get_datatype_for_pyarrow_lib(self):
        """Get the correlating datatype of the pyarrow library to the schema's UDF datatype.

        Returns:
            _type_: _description_
        """
        for data_type in data_types:
            if data_type.get_udf_type() == self._dataType:
                return data_type.get_pyarrow_type()

    def __str__(self) -> str:
        """Print this Class out in a readable way.

        Returns:
            str: String to print
        """
        return_str = f"\nIndex: {self._index}\n"
        return_str += f"Name: {self._name}\n"
        return_str += f"Size in Bytes: {self._sizeInBytes}\n"
        return_str += f"DataType: {self._dataType}\n"
        return_str += f"AxisName: {self._axisName}\n"
        return_str += f"ScalingFactor: {self._scalingFactor}\n"
        return_str += f"Amount of Values: {self._sizeInBytes}\n"
        # return_str += f"Values: {self._values}\n"
        return(return_str)

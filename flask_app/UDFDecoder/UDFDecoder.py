"""UDFDecoder."""
from .Schema import Schema
import struct
import pathlib
import typing
import pyarrow as pa
import pyarrow.parquet as pq
#import pyarrow.csv as csv


pyarrowSchema = typing.TypeVar("pyarrowSchema")
pyarrowTable = typing.TypeVar("pyarrowTable")


class UDFDecoder:
    """Class for reading binary files in the UDF Format.

    Defined at https://inside-docupedia.bosch.com/confluence/pages/viewpage.action?pageId=2282018346.
    """

    def __init__(self, time_format: str = "ns", debug_mode: bool = False) -> None:
        """Initialise a object of the UDFDecoder class.

        Args:
            time_format (str): String defining the time_format. Can only be one of the following: 'ns', 'us', 'ms' or 's'. 'ns'. Defaults to "ns".
            debug_mode (bool): Activates debug messages. Defaults to False.

        Raises:
            ValueError: If the TimeFormat is not supported
            TypeError: If the TimeFormat is not a string
            TypeError: If the debug_mode is not a string
        """
        if ["s", "ms", "us", "ns"].count(time_format) > 0:
            self._TimeFormat = time_format
        else:
            if type(time_format) == str:
                raise ValueError(f"TimeFormat '{time_format}' is not supported")
            raise TypeError(f"Argument TimeFormat '{time_format}' in constructor must be a string. It is of type {type(time_format)}")

        if type(debug_mode) == bool:
            self._DebugMode = debug_mode
        else:
            raise TypeError(f"Argument debug_mode '{debug_mode}' in constructor must be a string or integer. It is of type {type(debug_mode)}")

        self._PyArrowSchema = None
        self._PyArrowTable = None
        self._FilePath = None

    def __debug_message(self, debug_message: str):
        """Print a debug Message.

        Args:
            debug_message (str): Debug message to be printed.
        """
        if self._DebugMode:
            print("-------------------------------------------")
            print(debug_message)

    def __header_from_byte_array(self, file_blob: bytearray) -> tuple[list, int]:
        """Read the schemata from a UDF file header.

        Args:
            file_blob (bytearray): A bytearray that is only the header of a binary file in the UDF format.

        Returns:
            tuple[list, int]: A list of the schemata that are defined in the header and a cursor that is the Index of the first byte in the binary file after the header.
        """
        schemata = []
        variable_header = file_blob.decode("UTF-8").split("\r\n")
        self._Version = variable_header.pop(0)
        if self._Version == "1.0":
            for schema in variable_header:
                vals = schema.split(":")
                axis_names = vals[4].split(",")
                data_types = vals[3].split(",")
                size = int(int(vals[2]) / len(data_types))
                for axis_name in axis_names:
                    schemata.append(Schema(int(vals[0]), vals[1], size, data_types[axis_names.index(axis_name)].strip(), axis_name, float(vals[5]), -1, "na"))
            cursor = len(file_blob)
        elif self._Version == "1.1":
            for schema in variable_header:
                vals = schema.split(":")
                #print(vals)
                axis_names = vals[4].split(",")
                data_types = vals[3].split(",")
                size = int(int(vals[2]) / len(data_types))
                properties = vals[7].split(",")
                for axis_name in axis_names:
                    schemata.append(Schema(int(vals[0]), vals[1], size, data_types[axis_names.index(axis_name)].strip(), axis_name, float(vals[5]), float(vals[6]), properties))
            cursor = len(file_blob) + 6 # Skip the schema terminator
        else:
            print("Unsupported UDF Schema version")
            cursor = 0
        return schemata, cursor

    def __values_from_byte_array(self, file_blob: bytearray, cursor: int, schemata: list[Schema]) -> tuple[list[str], list[str], list[Schema]]:
        """Read the body of a UDF File.

        Args:
            file_blob (bytearray): A bytearray that is only the body of a binary file in the UDF format.
            cursor (int): A int that is the index of the first byte of the body in the binary file.
            schemata (list[Schema]): A list of schemata which were defined by the header of the binary file

        Raises:
            Exception: If the file_blob is unreadable

        Returns:
            tuple[list[str], list[str], list[Schema]]: A list of the timeStamps that are in the body, a list of the labels that are in the body and A list of the schemata which were defined in the header and have values in the body. Schemata without any data in the body are deleted.
        """
        timestamps = []
        labels = []
        while cursor < len(file_blob):
            if file_blob[cursor] == 0xF0 or file_blob[cursor] == 0xF1:
                cursor += 1
                timestamp_bytes = file_blob[cursor:cursor + 8]
                timestamps.append(str(struct.unpack("<Q", timestamp_bytes)[0]))
                labels.append(None)
                cursor += 8
            elif file_blob[cursor] == 0xF8:
                cursor += 1
                label_bytes = file_blob[cursor:cursor + 16]
                labels[len(labels)] = (struct.unpack("<s", label_bytes)[0])
                cursor += 8
            elif [schema.get_index() for schema in schemata].count(file_blob[cursor]) > 0:
                for schemata_index, schema in enumerate(schemata):
                    if cursor < len(file_blob) and file_blob[cursor] == schema.get_index():
                        schemas = [x for x in schemata if x.get_index() == file_blob[cursor]]
                        length = sum([x.get_size_in_bytes() for x in schemas])
                        cursor += 1
                        for i in range(0, int(length / schema.get_size_in_bytes())):
                            value_bytes = file_blob[cursor : (cursor + schema.get_size_in_bytes())]
                            if len(value_bytes) != schema.get_size_in_bytes():
                                print("Size mismatch")
                                break
                            if len(value_bytes) == 3: # Hack for u24 datatypes
                                vb_ba = bytearray(value_bytes)
                                vb_ba.append(0) # Add an MSB
                                value_bytes = bytes(vb_ba)
                            value = struct.unpack(schema.get_datatype_for_struct_lib(), value_bytes)[0]
                            schemata[schemata_index + i].add_value(value)
                            schemata[schemata_index + i].add_timestamp_index(len(timestamps) - 1)
                            cursor += schema.get_size_in_bytes()
            elif cursor < len(file_blob):
                raise Exception("Can't read Values from file. File may not be readable!")
        schemata = [schema for schema in schemata if schema.get_amount_of_values() > 0]
        self.__debug_message("Printing the first 10 values of every schema\n" + str([schema.get_values()[:10] for schema in schemata]))
        return timestamps, labels, schemata

    def __convert_time(self, time_stamps: list[str], time_format: str) -> list[float]:
        """Convert a list of timeStamps from nanoseconds to microseconds, milliseconds or seconds without loss of accuracy.

        Args:
            time_stamps (list[str]): A list of timeStamps. The timeStamps should all be strings.
            time_format (str): A string that is the time_format to which the timeStamps of the list are to be converted to.

        Returns:
            list[float]: A list of the timeStamps, which were converted. All timeStamps are now floats.
        """
        self.__debug_message("Converting the time")
        if time_format == "us":
            for index in range(0, len(time_stamps)):
                if len(time_stamps[index]) < 4:
                    time_stamps[index] = ("0" * (4 - len(time_stamps[index]))) + time_stamps[index]
                time_stamps[index] = time_stamps[index][0:len(time_stamps[index]) - 3] + "." + time_stamps[index][len(time_stamps[index]) - 3:]
        elif time_format == "ms":
            for index in range(0, len(time_stamps)):
                while len(time_stamps[index]) < 7:
                    time_stamps[index] = ("0" * (7 - len(time_stamps[index]))) + time_stamps[index]
                time_stamps[index] = time_stamps[index][0:len(time_stamps[index]) - 6] + "." + time_stamps[index][len(time_stamps[index]) - 6:]
        elif time_format == "s":
            for index in range(0, len(time_stamps)):
                while len(time_stamps[index]) < 10:
                    time_stamps[index] = ("0" * (10 - len(time_stamps[index]))) + time_stamps[index]
                time_stamps[index] = time_stamps[index][0:len(time_stamps[index]) - 9] + "." + time_stamps[index][len(time_stamps[index]) - 9:]
        for index in range(0, len(time_stamps)):
            time_stamps[index] = float(time_stamps[index])

        self.__debug_message("Printing the first 10 converted timestamps\n" + str(time_stamps[:10]))
        return time_stamps

    def __scale_values(self, pyarrow_table: pyarrowTable) -> pyarrowTable:
        """Take a PyArrow table and scales its values.

        Args:
            pyarrow_table (pyarrowTable): A PyArrow table with values and the scaling factors as metadata in the dataFields of the PyArrow schema of the table

        Returns:
            pyarrowTable: A PyArrow table with the scaled values
        """
        self.__debug_message("Scaling the Values")
        pyarrow_schema = pyarrow_table.schema
        metadata = pyarrow_schema.metadata
        metadata[b"Was Scaled"] = "True"
        data_fields = [pyarrow_schema[0]]
        for data_field in pyarrow_schema:
            if data_field.name != f"Time in {self._TimeFormat}":
                data_fields.append(data_field.with_type(pa.float64()))
        pyarrow_schema = pa.schema(data_fields).with_metadata(metadata)
        value_lists = [column.to_pylist() for column in pyarrow_table.columns]
        for value_list_index in range(1, len(value_lists)):
            for value_index in range(0, len(value_lists[value_list_index])):
                if value_lists[value_list_index][value_index] is not None:
                    value_lists[value_list_index][value_index] = value_lists[value_list_index][value_index] * float(pyarrow_schema.field(value_list_index).metadata[b'scaling_factor'].decode("UTF-8"))
        pyarrow_table = pa.Table.from_arrays(value_lists, schema=pyarrow_schema)
        self.__debug_message("Printing the scaled PyArrow table:\n" + str(pyarrow_table))
        return pyarrow_table

    def __make_pyarrow_schema(self, schemata: list, time_format: str) -> pa.Schema:
        """Create a pyarrow.schema object with data taken from a list of Schema objects and a time_format defined by a string.

        The pyarrow schema will look something like this.:

        | Time in '<defined by param>' | First Schema from the list of Schema | ... | Last Schema from the list of Schema |

        Args:
            schemata (list): list of schemata defined in the header of the UDF file.
            time_format (str): string defining the time_format. If it is not 'ns'. It is expected that the timestamps are floats.

        Returns:
            pa.Schema: A pyarrow.schema object.
        """
        data_fields = [pa.lib.field(schema.get_name() + "." + schema.get_axis_name(), schema.get_datatype_for_pyarrow_lib(), metadata={"scaling_factor": str(schema.get_scaling_factor())}) for schema in schemata]
        if time_format == "ns":
            data_fields.insert(0, pa.lib.field(f'Time in {self._TimeFormat}', pa.uint64(), metadata={"scaling_factor": "1.0"}))
        else:
            data_fields.insert(0, pa.lib.field(f'Time in {self._TimeFormat}', pa.float64(), metadata={"scaling_factor": "1.0"}))
        data_fields.insert(1, pa.lib.field('Labels', pa.string(), metadata={"scaling_factor": "1.0"}))
        return pa.schema(data_fields)

    def __make_pyarrow_table(self, pyarrow_schema: pa.schema, schemata: list[Schema], time_stamps: list, labels: list) -> pa.Table:
        """Create a pyarrow.table object with data taken from a list of Schema objects and a list of time_stamps. The schema of the table is defined by a pyarrow.schema object.

        Args:
            pyarrow_schema (pa.schema): A PyArrow Schema which correlates to the other two parameters.
            schemata (list[Schema]): A list of Schema objects which all have values in their lists of values.
            time_stamps (list): A list of all time_stamps of the binary file.
            labels (list): A list of all labels in the binary file.

        Returns:
            pa.Table: A pyarrow.table object created from the parameters.
        """
        all_value_lists = [[None for time_stamp in time_stamps] for i in range(0, len(schemata))]
        for all_value_lists_index, value_list in enumerate(all_value_lists):
            for value_index, value in enumerate(schemata[all_value_lists_index].get_values()):
                value_list[schemata[all_value_lists_index].get_timestamp_indices()[value_index]] = value
        all_value_lists.insert(0, time_stamps)
        all_value_lists.insert(1, labels)
        pyarrow_table = pa.Table.from_arrays(all_value_lists, schema=pyarrow_schema)

        self.__debug_message("Printing the PyArrow table:\n" + str(pyarrow_table))
        return pyarrow_table

    def read_bin_file(self, file_path) -> None:
        """Read the binary file in the UDF Format that is given as a parameter.

        Args:
            file_path (_type_): Path to file that should be read.

        Raises:
            FileNotFoundError: If the file was not found
            TypeError: If the filepath was not a string
        """
        if type(file_path) == str:
            self._FilePath = pathlib.Path(file_path)
            if not self._FilePath.exists():
                raise FileNotFoundError(f"File {self._FilePath} was not found")
        else:
            raise TypeError(f"Argument FilePath in constructor must be a string. It is of type {type(file_path)}")
        with open(self._FilePath, 'rb') as f:
            file_blob = f.read()
        schemata, end_of_header = self.__header_from_byte_array(file_blob[:file_blob.find("\r\n\r\n".encode("UTF-8"))])
        time_stamps, labels, schemata = self.__values_from_byte_array(file_blob, end_of_header, schemata)
        if self._TimeFormat != "ns":
            time_stamps = self.__convert_time(time_stamps, self._TimeFormat)
        else:
            time_stamps = [int(time_stamp) for time_stamp in time_stamps]
        self._PyArrowSchema = self.__make_pyarrow_schema(schemata, self._TimeFormat)
        was_scaled = {"Was Scaled": "False"}
        self._PyArrowSchema = self._PyArrowSchema.with_metadata(was_scaled)
        self._PyArrowTable = self.__make_pyarrow_table(self._PyArrowSchema, schemata, time_stamps, labels)

    def get_arrow_schema(self) -> pyarrowSchema:
        """Return the PyArrow schema that was created by calling read_bin_file().

        Raises:
            Exception: If this function is called before read_bin_file()

        Returns:
            pyarrowSchema: A PyArrow schema
        """
        if self._PyArrowSchema is not None:
            return self._PyArrowSchema
        else:
            raise Exception("File has to be read before getting the PyArrow schema")

    def get_arrow_table(self) -> pyarrowTable:
        """Return the PyArrow table that was created by calling read_bin_file().

        Raises:
            Exception: If this function is called before read_bin_file()

        Returns:
            pyarrowTable: A PyArrow table
        """
        if self._PyArrowTable is not None:
            return self._PyArrowTable
        else:
            raise Exception("File has to be read before getting the PyArrow Table")

    def write_parquet_file(self, apply_scaling: bool):
        """Write the PyArrow table that was created by calling read_bin_file() into a .parquet file.

        Args:
            apply_scaling (bool): Should the values be scaled according to the scaling factors in the PyArrow schema of the PyArrow table
        """
        if apply_scaling:
            scaled_table = self.__scale_values(self._PyArrowTable)
            pq.write_table(scaled_table, self._FilePath.with_suffix(".parquet"))
        else:
            pq.write_table(self._PyArrowTable, self._FilePath.with_suffix(".parquet"))
        self.__debug_message("Wrote PyArrowTable into a Parquet file")

    def write_csv_file(self, apply_scaling: bool = True):
        """Write the PyArrow table that was created by calling read_bin_file() into a .parquet file.

        Args:
            apply_scaling (bool): Should the values be scaled according to the scaling factors in the PyArrow schema of the PyArrow table. Defaults to True.
        """
        if apply_scaling:
            scaled_table = self.__scale_values(self._PyArrowTable)
            with csv.CSVWriter(self._FilePath.with_suffix(".csv"), scaled_table.schema) as writer:
                writer.write_table(scaled_table)
        else:
            with csv.CSVWriter(self._FilePath.with_suffix(".csv"), self._PyArrowTable.schema) as writer:
                writer.write_table(self._PyArrowTable)
        self.__debug_message("Wrote PyArrowTable into a CSV file")

    def add_user_meta_data(self, meta_data: dict):
        """Add user defined meta_data to the meta_data of the PyArrowSchema.

        Args:
            meta_data (dict): A python dict that contains the meta_data that should be added to the schema
        """
        meta_data = (meta_data | self._PyArrowSchema.metadata)
        self._PyArrowTable = self._PyArrowTable.replace_schema_metadata(meta_data)
        self._PyArrowSchema = self._PyArrowTable.schema

    def reset(self):
        """Reset the pyArrow table and schema of this object."""
        self._PyArrowSchema = None
        self._PyArrowTable = None
        self._FilePath = None
        self.__debug_message("Reset: Set FilePath, PyArrow schema and PyArrow table to None.")
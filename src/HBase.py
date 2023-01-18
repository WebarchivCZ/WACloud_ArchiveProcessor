#!/usr/bin/python
# coding: utf-8

"""..module:: archiveprocessor.HBase.

..moduleauthor:: Jan Lehecka <jlehecka@ntis.zcu.cz>
"""
from typing import Dict, Any, Optional, Generator, List
import json

import happybase
import bson

from BaseAlgorithms import BaseAlgorithm
from config import (
    HBASE_MAIN_TABLE,
    HBASE_HARV_TABLE,
    HBASE_CONF_TABLE,
    HBASE_PROC_TABLE
)


class HBase(BaseAlgorithm):
    """Module for handling HBase operations using happybase package.

    Requirements:
        happybase python package
            $ pip install happybase
        Apache Thrift server running
            $ hbase-daemon.sh start thrift -p PORT

    """

    def _init(self, host: str, port: int) -> None:
        """Class constructor."""
        self.host = host
        self.port = port
        self.conn = happybase.Connection(host, port=port)
        self.CACHE = {}

    def close(self) -> None:
        """Close the connection with HBase."""
        self.conn.close()

    def restart_connection(self) -> None:
        """Restart the connection with HBase."""
        self.conn.close()
        self.conn = happybase.Connection(self.host, port=self.port)
        self.CACHE = {}

    def get_tables(self) -> set:
        """Return set of all HBase table names."""
        return set(self.conn.tables())

    def has_table(self, table: str) -> bool:
        """Check if HBase table with given name exists.

        Args:
            table: The name of the table.

        Returns:
            True if the table exists, False otherwise.

        """
        table = self.to_bytes(table)
        return table in self.get_tables()

    def create_table(self, name: str, max_versions: int = 1) -> None:
        """Create HBase table with single column family.

        Args:
            name: The name of the table.
            max_versions: Maximum number of historical versions to keep in
                HBase.

        """
        name = self.to_bytes(name)
        families = {
            'cf1': dict(max_versions=max_versions),
            }
        self.conn.create_table(name, families)

    def check_table(self, name: str, max_versions: int = 1) -> None:
        """Create and/or enable table if necessary.

        Args:
            name: The name of the table.
            max_versions: Maximum number of historical versions to keep in
                HBase.

        """
        name = self.to_bytes(name)
        if self.has_table(name) and self.conn.is_table_enabled(name):
            self.logger.info(f'Checking HBase table {name} ... OK.')
            return
        if not self.has_table(name):
            self.create_table(name, max_versions)
            self.logger.info(f'HBase table {name} successfully created.')
        if not self.conn.is_table_enabled(name):
            self.conn.enable_table(name)
            self.logger.info(f'HBase table {name} successfully enabled.')

    def disable_table(self, name: str) -> None:
        """Disable table with given name.

        Args:
            name: The name of the table.

        """
        name = self.to_bytes(name)
        self.conn.disable_table(name)

    def check_tables(self) -> None:
        """Check all tables. Create and/or enable if necessary."""
        self.logger.info(f'Checking HBase tables:')
        self.check_table(HBASE_MAIN_TABLE, 1)
        self.check_table(HBASE_HARV_TABLE, 1)
        self.check_table(HBASE_CONF_TABLE, 100)
        self.check_table(HBASE_PROC_TABLE, 1)

    def delete_table(self, name: str) -> None:
        """Delete table with given name.

        Args:
            name: The name of the table.

        """
        name = self.to_bytes(name)
        try:
            self.conn.delete_table(name, disable=True)
            self.logger.info(f'HBase table {name} successfully deleted.')
        except Exception as e:
            self.logger.error(f'Failed to delete HBase table {name}: {e}.')

    def get_table(self, name: str) -> happybase.Table:
        """Return instance of happybase.Table with given name.

        Args:
            name: The name of the table.

        """
        name = self.to_bytes(name)
        try:
            # try to use cached instance of the table
            table = self.CACHE[name]
        except KeyError:
            table = self.conn.table(name)
            self.CACHE[name] = table
        return table

    def put(
          self,
          table_name: str,
          key: str,
          data: Dict[str, Any],
          max_fails: int = 3,
          dict2bson: bool = True,
          **kwargs
          ) -> bool:
        """Save row into given table.

        Do not rely on Thrift server to convert data into bytes, do it
        explicitly here.

        Args:
            table_name: The name of the table.
            key: The key of the row.
            data: Dictionary with the row data.
            max_fails: Maximum number of failed attempts before giving up.
            dict2bson: Whether to convert possible dictionaries in the data
                values into BSON format.

        Returns:
            True if the row was successfully saved, False otherwise.

        """
        table = self.get_table(table_name)
        key = self.to_bytes(key)
        data_bytes = {}
        for k, v in data.items():
            k = self.to_bytes(k)
            if not k.startswith(b'cf1:'):
                k = b'cf1:' + k
            v = self.to_bytes(v, dict2bson=dict2bson)
            data_bytes[k] = v

        N_fails = 0
        while True:
            try:
                table = self.get_table(table_name)
                table.put(key, data_bytes, **kwargs)
                return True
            except Exception as e:
                N_fails += 1
                err = (
                    f'{N_fails}. fail to save HBase row with key {key} ({e}).'
                )
                if N_fails < max_fails:
                    self.restart_connection()
                    self.logger.warning(
                        f"{err} Trying to repeat the operation with restarted"
                        f" connection."
                    )
                else:
                    self.logger.error(
                        f"{err} Reached maximum number of fails. Record has "
                        f"not been saved!"
                    )
                    return False

    def get_row(
          self,
          table_name: str,
          key: str,
          **kwargs
          ) -> Optional[Dict[bytes, bytes]]:
        """Return the content of row with given key.

        Args:
            table_name: The name of the table.
            key: The key of the row.

        Returns:
            The row dict, or None if failed to load the row.

        """
        table = self.get_table(table_name)
        key = self.to_bytes(key)
        try:
            return table.row(key, **kwargs)
        except Exception as e:
            self.logger.error(f'Failed to get HBase row with key {key}: {e}')

    def has_row(
          self,
          table_name: str,
          key: str,
          **kwargs
          ) -> bool:
        """Check if the table has row with given key.

        Args:
            table_name: The name of the table.
            key: The key of the row.

        Returns:
            Whether the table has row with given key.

        """
        r = self.get_row(table_name, key)
        return bool(r)

    def get_rows_by_prefix(
          self,
          table_name: str,
          prefix: str,
          **kwargs
          ) -> Generator:
        """Scan HBase for keys with given prefix.

        Args:
            table_name: The name of the table.
            prefix: The key prefix for happybase's scan function. Set prefix=""
                to get all rows from the table.

        Returns:
            Generator yielding the rows matching the scan.

        """
        table = self.get_table(table_name)
        prefix = self.to_bytes(prefix)
        try:
            return table.scan(row_prefix=prefix, **kwargs)
        except Exception as e:
            self.logger.error(
                f'Failed to scan HBase rows with prefix {prefix}: {e}'
            )

    def get_cell_versions(
          self,
          table_name: str,
          row: str,
          column: str,
          **kwargs
          ) -> Optional[List]:
        """Get all stored versions of given cell.

        Args:
            table_name: The name of the table.
            row: The key of the row.
            column: The name of the column.

        Returns:
            cells: List of cell values as returned by happybase.table.cells.

        """
        table = self.get_table(table_name)
        row = self.to_bytes(row)
        column = self.to_bytes(column)
        try:
            return table.cells(row, column, **kwargs)
        except Exception as e:
            self.logger.error(
                f'Failed to get versions from HBase table {table_name}, '
                f'row {row} and column {column}: {e}'
            )

    def put_rows_from_json_file(
          self,
          table_name: str,
          fn: str,
          dict2bson: bool = False
          ) -> None:
        """Save data from given JSON file into HBase table.

        JSON file is expected to be a dict of {row-key: data} pairs.

        Args:
            table_name: The name of the table.
            fn: Path to JSON file.
            dict2bson: Whether to convert possible dictionaries in the data
                values into BSON format.

        """
        data = json.load(open(fn, "r"))
        nok = 0
        for key, rowdata in data.items():
            success = self.put(table_name, key, rowdata, dict2bson=dict2bson)
            nok += success
        if nok:
            self.logger.info(
                f'Successfully updated {len(list(data.keys()))} rows in HBase '
                f'table {table_name}.'
            )

    def to_bytes(self, obj: Any, dict2bson: bool = True) -> bytes:
        """Serialize object into bytes.

        Based on the type of the object, suitable converting method is
        selected:
        - direct byte conversion for strings,
        - BSON for dictionaries,
        - JSON followed by byte conversion for other types.

        Args:
            obj: Object to be serialized.
            dict2bson: Whether to convert possible dictionaries in the data
                values into BSON format.

        Returns:
            Object serialized into bytes.

        """
        def _str_to_bytes(string):
            if isinstance(string, bytes):
                return string
            try:
                return bytes(string, "utf-8")
            except Exception as e:
                self.logger.warning(
                    f'Failed to convert string "{string}" into bytes: {e}.'
                )
                try:
                    return bytes(string, "utf-8", errors="replace")
                except Exception:
                    return b''

        def _dumps(module, obj):
            try:
                return module.dumps(obj)
            except Exception as e:
                self.logger.warning(
                    f'Failed to convert object "{obj}" into '
                    f'{module.__name__}: {e}.'
                )
                return b""

        if isinstance(obj, bytes):
            return obj
        elif isinstance(obj, str):
            return _str_to_bytes(obj)
        elif isinstance(obj, dict) and dict2bson:
            # BSON is more effective for dictionaries
            return _dumps(bson, obj)
        else:
            # the rest of objects (int, float, list, tuple etc.) -> JSON
            # -> bytes
            obj_str = _dumps(json, obj)
            return _str_to_bytes(obj_str)

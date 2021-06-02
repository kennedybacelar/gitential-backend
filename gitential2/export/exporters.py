from abc import abstractmethod
import csv
import os
import json
from collections import defaultdict

from typing import Optional, List, Dict
from pathlib import Path
from gitential2.datatypes.export import ExportableModel


class Exporter:
    def export_object(self, obj: ExportableModel, fields: Optional[List[str]] = None):
        fields = fields or obj.export_fields()
        name_singular, name_plural = obj.export_names()
        exportable_dict = obj.to_exportable(fields=fields)
        self._export_low_level(
            name_singular=name_singular, name_plural=name_plural, fields=fields, exportable_dict=exportable_dict
        )

    @abstractmethod
    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        pass

    def close(self):
        pass


class CSVExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self._files: dict = {}
        self._writers: dict = {}

    def _get_filename(self, name_plural: str):
        return os.path.join(self.destination_directory, f"{self.prefix}{name_plural}.csv")

    def _get_writer(self, name_singular: str, name_plural: str, fields: List[str]):
        if name_singular not in self._writers:
            self._files[name_singular] = open(self._get_filename(name_plural), "w")
            self._writers[name_singular] = csv.DictWriter(self._files[name_singular], fieldnames=fields)
            self._writers[name_singular].writeheader()
        return self._writers[name_singular]

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        writer = self._get_writer(name_singular, name_plural, fields)
        writer.writerow(exportable_dict)

    def close(self):
        for f in self._files.values():
            f.close()


class JSONExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self._files: dict = {}
        self._counter: Dict[str, int] = defaultdict(int)

    def _get_filename(self, name_plural: str):
        return os.path.join(self.destination_directory, f"{self.prefix}{name_plural}.json")

    def _get_json_file(self, name_singular, name_plural):
        if name_singular not in self._files:
            self._files[name_singular] = open(self._get_filename(name_plural), "w")
            self._files[name_singular].write("[\n")
        return self._files[name_singular]

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        json_str = json.dumps(exportable_dict, sort_keys=False, indent=2)
        json_file = self._get_json_file(name_singular, name_plural)
        self._counter[name_singular] += 1
        if self._counter[name_singular] > 1:
            json_file.write(",\n")
        json_file.write(json_str)

    def close(self):
        for f in self._files.values():
            f.write("]")
            f.close()

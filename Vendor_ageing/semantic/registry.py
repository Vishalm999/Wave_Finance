#vendor_ageing/semantic/registry.py

from dataclasses import dataclass
from typing import Dict, List, Optional
import yaml
import os

@dataclass(frozen=True)
class Fact:
    name: str
    table: str
    grain: str
    primary_date: str

@dataclass(frozen=True)
class Dimension:
    name: str
    column: str
    type: str

@dataclass(frozen=True)
class Measure:
    name: str
    column: str
    aggregation: str
    sql: Optional[str] = None

@dataclass(frozen=True)
class Metric:
    name: str
    description: str
    expression: str
    behavior: str

class SemanticRegistry:
    def __init__(self, model_path: str):
        self.model_path = model_path
        
        self.fact: Fact = None
        self.dimensions: Dict[str, Dimension] = {}
        self.measures: Dict[str, Measure] = {}
        self.metrics: Dict[str, Metric] = {}
        
        self._load_all()

    def _load_yaml(self, filename: str) -> dict:
        path = os.path.join(self.model_path, filename)
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _load_all(self):
        self._load_fact()
        self._load_dimensions()
        self._load_measures()
        self._load_metrics()

    def _load_fact(self):
        data = self._load_yaml("facts.yaml")
        name, cfg = next(iter(data.items()))
        self.fact = Fact(
            name=name,
            table=cfg["table"],
            grain=cfg["grain"],
            primary_date=cfg["primary_date"],
        )

    def _load_dimensions(self):
        data = self._load_yaml("dimensions.yaml")
        for name, cfg in data.items():
            self.dimensions[name] = Dimension(
                name=name,
                column=cfg["column"],
                type=cfg["type"],
            )

    def _load_measures(self):
        data = self._load_yaml("measures.yaml")
        for name, cfg in data.items():
            self.measures[name] = Measure(
                name=name,
                column=cfg["column"],
                aggregation=cfg["aggregation"],
                sql=cfg.get("sql")
            )

    def _load_metrics(self):
        data = self._load_yaml("metrics.yaml")
        for name, cfg in data.items():
            self.metrics[name] = Metric(
                name=name,
                description=cfg.get("description", ""),
                expression=cfg["measure"],
                behavior=cfg["behavior"]
            )

    # Add this method to your SemanticRegistry class

    def register_all_columns_as_dimensions(self, columns: List[str]):
        """Dynamically register all columns as dimensions"""
        from dataclasses import dataclass
    
        @dataclass
        class Dimension:
            name: str
            column: str
            type: str = "string"
            description: str = ""
    
        for column in columns:
            dim_name = self._column_to_dimension_name(column)
            if dim_name not in self.dimensions:
            # Determine data type (you may need to query this from the database)
                data_type = "string"  # default
                if column in ["Vendor_no", "Company_Code", "Delay_days"]:
                    data_type = "integer"
                elif column in ["Overdue"]:
                    data_type = "double"
            
                self.dimensions[dim_name] = Dimension(
                    name=dim_name,
                    column=column,
                    type=data_type,
                    description=f"Dimension for {column}"
                )

    def _column_to_dimension_name(self, column: str) -> str:
        """Convert column name to dimension name"""
        # Vendor_no -> vendor_no
        # Profit_Centre_text -> profit_centre_text
        
        return column.lower()
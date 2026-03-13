#vendor_ageing/semantic/validator.py

from typing import List
from semantic.registry import SemanticRegistry

class SemanticValidationError(Exception):
    pass

class SemanticValidator:
    def __init__(self, registry: SemanticRegistry):
        self.registry = registry

    def validate(self, intent) -> List[str]:
        warnings = []
        
        # Validate metric
        metric = getattr(intent, 'metric', None)
        if metric and metric not in self.registry.metrics:
            warnings.append(f"Unknown metric: {metric} - using default")
        
        # Validate dimensions
        dimensions = getattr(intent, 'dimensions', [])
        for dim in dimensions:
            if dim not in self.registry.dimensions:
                warnings.append(f"Unknown dimension: {dim}")
        
        # Validate filters
        filters = getattr(intent, 'filters', {})
        for key in filters:
            if key not in self.registry.dimensions:
                warnings.append(f"Filter key not a dimension: {key}")
        
        return warnings
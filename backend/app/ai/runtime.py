# Deprecated.
# Use backend.app.ai.runtimes instead.

import warnings
from .runtimes.transformers_runtime import TransformersGemmaRuntime

# Emit a runtime deprecation warning when this module is imported
warnings.warn(
    "Importing LocalGemmaRuntime from backend.app.ai.runtime is deprecated. "
    "Please use backend.app.ai.runtimes instead.",
    DeprecationWarning,
    stacklevel=2
)

# Export class name alias for backward compatibility
LocalGemmaRuntime = TransformersGemmaRuntime

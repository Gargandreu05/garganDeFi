import os
from setuptools import setup, Extension
import pybind11

# The Extension module definition
ext_modules = [
    Extension(
        "core_math",
        ["defi_engine/core_math.cpp"],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=["/std:c++17", "/O2"] if os.name == "nt" else ["-std=c++17", "-O3"],
    ),
]

setup(
    name="core_math",
    version="1.0",
    author="GarganTech",
    description="High-performance C++ math engine for GarganDeFi",
    ext_modules=ext_modules,
)

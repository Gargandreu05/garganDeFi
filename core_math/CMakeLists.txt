cmake_minimum_required(VERSION 3.12)
project(core_math)

set(CMAKE_CXX_STANDARD 17)

# Download pybind11 automatically using FetchContent
include(FetchContent)
FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG        v2.11.1
)
FetchContent_MakeAvailable(pybind11)

# Add the module
pybind11_add_module(core_math core_math.cpp)

# Optimize for speed
target_compile_options(core_math PRIVATE -O3)

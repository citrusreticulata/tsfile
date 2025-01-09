# toolchain-arm.cmake
# 配置交叉编译工具链，使用 ARM 编译器
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

# 自己下载的, 最终使用的是13.3.rel1 ===== !! =====
# 2025.01.06记录. 似乎交叉编译的uuid库还是找不到
set(tools /home/ubuntu/software/arm-gnu-toolchain-13.3.rel1-x86_64-arm-none-linux-gnueabihf)
set(CMAKE_C_COMPILER ${tools}/bin/arm-none-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER ${tools}/bin/arm-none-linux-gnueabihf-g++)
set(CMAKE_FIND_ROOT_PATH ${tools}/lib)
message(STATUS "CMAKE_FIND_ROOT_PATH= ${CMAKE_FIND_ROOT_PATH}")
include_directories(${tools}/include)
# include_directories(${tools}/include/libuuid)
# find_library(uuid NAMES uuid PATHS ${CMAKE_FIND_ROOT_PATH} NO_DEFAULT_PATH REQUIRED)
# message(STATUS "Found tsfile library: ${uuid}")
# message(STATUS "UUID_LIBRARIES: ${UUID_LIBRARIES}")
# message(FATAL_ERROR "UUID_LIBRARY_DIRS: ${UUID_LIBRARY_DIRS}")


# set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
# set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)

# set(CMAKE_CXX_STANDARD 11)  # 强制使用 C++11 标准
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_EXTENSIONS OFF)  # 禁用编译器特有扩展
# find_library(PATHS /usr/lib/gcc/x86_64-linux-gnu/11)


# toolchain-arm.cmake
# 配置交叉编译工具链，使用 ARM 编译器
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

# cyber7001交叉编译链 gcc-linaro-5.5.0-2017.10-x86_64_arm-linux-gnueabi.zip
# set(tools /home/ubuntu/software/gcc-linaro-5.5.0-2017.10-x86_64_arm-linux-gnueabi)
# set(CMAKE_C_COMPILER ${tools}/bin/arm-linux-gnueabi-gcc)
# set(CMAKE_CXX_COMPILER ${tools}/bin/arm-linux-gnueabi-g++)
# set(CMAKE_FIND_ROOT_PATH ${tools}/lib)

# 更新一点的 TQ3588-gcc-linaro-11.3.0.tar.bz2
# 2025.01.06记录. 可以初步交叉编译，能过大小端、gcc版本等检查
set(tools /home/ubuntu/software/TQ3588-gcc-linaro-11.3.0/toolchain)
set(CMAKE_C_COMPILER ${tools}/bin/aarch64-linux-gcc)
set(CMAKE_CXX_COMPILER ${tools}/bin/aarch64-linux-g++)
set(CMAKE_FIND_ROOT_PATH ${tools}/lib)
include_directories(${tools}/include)
# include_directories(${tools}/include/libuuid)

# 自己下载的, 最终使用的是13.3.rel1 ===== !! =====
# 2025.01.06记录. 似乎交叉编译的uuid库还是找不到
# set(tools /home/ubuntu/software/arm-gnu-toolchain-13.3.rel1-x86_64-arm-none-linux-gnueabihf)
# set(CMAKE_C_COMPILER ${tools}/bin/arm-none-linux-gnueabihf-gcc)
# set(CMAKE_CXX_COMPILER ${tools}/bin/arm-none-linux-gnueabihf-g++)
# set(CMAKE_FIND_ROOT_PATH ${tools}/lib)
# include_directories(${tools}/include)
# include_directories(${tools}/include/libuuid)

# set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
# set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)

# 自己下载的
# set(tools /home/ubuntu/software/arm-gnu-toolchain-11.3.rel1-x86_64-arm-none-eabi)
# set(CMAKE_C_COMPILER ${tools}/bin/arm-none-eabi-gcc)
# set(CMAKE_CXX_COMPILER ${tools}/bin/arm-none-eabi-g++)
# set(CMAKE_FIND_ROOT_PATH ${tools}/lib)

# set(CMAKE_CXX_STANDARD 11)  # 强制使用 C++11 标准
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_EXTENSIONS OFF)  # 禁用编译器特有扩展
# find_library(PATHS /usr/lib/gcc/x86_64-linux-gnu/11)


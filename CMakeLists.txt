#
#  mu2egrid/CMakeLists.txt
#
#  CMake build file for mu2egrid
#  This simply copies files into an install directory - there is no build
#
#  install steps:
#  cd /path/to/build/directory
#  source /path/to/mu2egrid/ups/setup_for_development 
#  cmake [-DCMAKE_INSTALL_PREFIX=/install/path] $CETPKG_SOURCE
#  make install
#  make package (tar file for ups)

CMAKE_MINIMUM_REQUIRED (VERSION 2.8)

PROJECT (mu2egrid)

# cetbuildtools contains our cmake modules
set (CETBUILDTOOLS_VERSION $ENV{CETBUILDTOOLS_VERSION})
if(NOT CETBUILDTOOLS_VERSION)
  message(FATAL_ERROR "ERROR: setup cetbuildtools to get the cmake modules")
endif()
set(CMAKE_MODULE_PATH $ENV{CETBUILDTOOLS_DIR}/Modules ${CMAKE_MODULE_PATH})

include(CetCMakeEnv)
# mu2egrid has no architecture dependence
cet_cmake_env( noarch )

find_ups_product(cetbuildtools v2_04_00)

# source code
add_subdirectory(bin)
add_subdirectory(ups)

# packaging utility
include(UseCPack)


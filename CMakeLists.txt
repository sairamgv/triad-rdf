 #
 # Copyright 2014 Sairam Gurajada
 #
 # Licensed under the Apache License, Version 2.0 (the "License");
 # you may not use this file except in compliance with the License.
 # You may obtain a copy of the License at
 #
 # http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.
 #

cmake_minimum_required(VERSION 2.6)

project(TriAD C CXX)

# add a NativeRelease build type
SET( CMAKE_CXX_FLAGS_NATIVERELEASE "-O3 -DNDEBUG -march=native -ftree-vectorize" CACHE STRING
    "Flags used by the C++ compiler during native builds."
    FORCE )
SET( CMAKE_C_FLAGS_NATIVERELEASE "-O3 -DNDEBUG -march=native -ftree-vectorize" CACHE STRING
    "Flags used by the C compiler during native builds."
    FORCE )
SET( CMAKE_EXE_LINKER_FLAGS_NATIVERELEASE "" CACHE STRING
    "Flags used for linking binaries during native builds."
    FORCE )
SET( CMAKE_SHARED_LINKER_FLAGS_NATIVERELEASE "" CACHE STRING
    "Flags used by the shared libraries linker during native builds."
    FORCE )
MARK_AS_ADVANCED(
    CMAKE_CXX_FLAGS_NATIVERELEASE
    CMAKE_C_FLAGS_NATIVERELEASE
    CMAKE_EXE_LINKER_FLAGS_NATIVERELEASE
    CMAKE_SHARED_LINKER_FLAGS_NATIVERELEASE )

if(CMAKE_CONFIGURATION_TYPES)
  list(APPEND CMAKE_CONFIGURATION_TYPES NativeRelease)
  list(REMOVE_DUPLICATES CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_CONFIGURATION_TYPES "${CMAKE_CONFIGURATION_TYPES}" CACHE STRING
     "Add the native configuration"
     FORCE)
endif()

## default build type is native
IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE NativeRelease CACHE STRING
      "Choose the type of build. Options are: None Debug NativeRelease Release RelWithDebInfo MinSizeRel."
      FORCE)
ENDIF(NOT CMAKE_BUILD_TYPE)
MESSAGE(STATUS "Build type: " ${CMAKE_BUILD_TYPE})

# handy stuff for eclipse
IF(CMAKE_COMPILER_IS_GNUCC)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fmessage-length=0")
ENDIF(CMAKE_COMPILER_IS_GNUCC)
IF(CMAKE_COMPILER_IS_GNUCXX)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fmessage-length=0")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

# definitions for visual studio
add_definitions(-D_CRT_SECURE_NO_WARNINGS)

# my own includes/libs
include_directories(~/local/include)
link_directories(~/local/lib)

# MPI
FIND_PACKAGE(MPI REQUIRED)
SET(CMAKE_C_COMPILER ccache)
SET(CMAKE_C_COMPILER_ARG1 mpicc)
SET(CMAKE_CXX_COMPILER ccache)
SET(CMAKE_CXX_COMPILER_ARG1 mpicxx)

# search for boost library
##find_package(Boost 1.46.1 COMPONENTS serialization mpi thread)
##IF(NOT ${Boost_FOUND})
	SET(BOOST_ROOT ~/local) # default
	SET(Boost_NO_SYSTEM_PATHS TRUE) # needed with cmake on lenny to prevent it grabbing an old system version...
	find_package(Boost 1.47 COMPONENTS serialization mpi thread program_options regex system filesystem chrono REQUIRED)
##ENDIF(NOT ${Boost_FOUND})
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})

# include directories
include_directories(${TriAD_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${TriAD_SOURCE_DIR}/extern/include ${TriAD_SOURCE_DIR}/include)

# install directories
SET(LIB_INSTALL_DIR lib)
SET(INCLUDE_INSTALL_DIR include)

# go
configure_file("${TriAD_SOURCE_DIR}/log4j.properties" "${CMAKE_CURRENT_BINARY_DIR}/")
#add_subdirectory(rg)
#add_subdirectory(mpi2)
add_subdirectory(utils)
add_subdirectory(parser)
add_subdirectory(database)
add_subdirectory(cts)
add_subdirectory(queryengine)
add_subdirectory(processor)
add_subdirectory(tools)

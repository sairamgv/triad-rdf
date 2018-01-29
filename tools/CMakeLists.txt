add_subdirectory(graphformats)

# include sources
include_directories (${TriAD_SOURCE_DIR}/tools/triad)

# link sources
link_directories (${TriAD_BINARY_DIR}/tools/triad)

#link libraries
link_libraries(TriAD gFormats ${Boost_LIBRARIES} rg mpi2 cts log4cxx)

#Build target
add_executable (triad triad/triADMain.cpp)
add_executable (metisformat triad/gFormats.cpp)

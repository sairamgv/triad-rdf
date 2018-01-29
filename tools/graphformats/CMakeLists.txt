# include sources
include_directories (${TriAD_SOURCE_DIR}/graphformats)

# link sources
link_directories (${TriAD_BINARY_DIR}/graphformats)

link_libraries(${Boost_LIBRARIES} mpi2 log4cxx Database cts Parser Utils)

#Build target
add_library(gFormats MetisFormat.cpp )

# link with boost
target_link_libraries(gFormats TriAD ${Boost_LIBRARIES} rg Database QueryEngine log4cxx)

# install library and main headers
install(TARGETS gFormats DESTINATION ${LIB_INSTALL_DIR})
# include sources
include_directories (${TriAD_SOURCE_DIR}/processor)

# link sources
link_directories (${TriAD_BINARY_DIR}/processor)

link_libraries(${Boost_LIBRARIES} mpi2 log4cxx Database QueryEngine cts Parser Utils)

#Build target
add_library(TriAD TriADProcessor.cpp)

# link with boost
target_link_libraries(TriAD ${Boost_LIBRARIES} rg Database QueryEngine log4cxx)

# install library and main headers
install(TARGETS TriAD DESTINATION ${LIB_INSTALL_DIR})



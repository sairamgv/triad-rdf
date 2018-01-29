# include sources
include_directories (${TriAD_SOURCE_DIR}/queryengine)

# link sources
link_directories (${TriAD_BINARY_DIR}/queryengine)

#link libraries
link_libraries(${Boost_LIBRARIES} rg mpi2 log4cxx Database Parser Utils cts)

file(COPY "${TriAD_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#Build libraries
add_library(QueryEngine QueryEngine.cpp Relation.cpp Operator.cpp)

# link with boost
target_link_libraries(QueryEngine ${Boost_LIBRARIES})

# install library and main headers
install(TARGETS QueryEngine DESTINATION ${LIB_INSTALL_DIR})

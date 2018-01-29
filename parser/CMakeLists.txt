# include sources
include_directories (${TriAD_SOURCE_DIR}/utils)

# link sources
link_directories (${TriAD_BINARY_DIR}/utils)

#link libraries
link_libraries(${Boost_LIBRARIES} mpi2 log4cxx)

file(COPY "${TriAD_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#Build libraries
add_library(Parser Parser.cpp)

# link with boost
target_link_libraries(Parser ${Boost_LIBRARIES})

#install targets
install(TARGETS Parser DESTINATION ${LIB_INSTALL_DIR})

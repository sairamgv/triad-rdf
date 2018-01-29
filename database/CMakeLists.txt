# include sources
include_directories (${TriAD_SOURCE_DIR}/database)

# link sources
link_directories (${TriAD_BINARY_DIR}/database)

#link libraries
link_libraries(${Boost_LIBRARIES} rg mpi2 log4cxx Parser Utils)

file(COPY "${TriAD_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#Build libraries
add_library(Database Database.cpp Dictionary.cpp TripleStore.cpp Statistics.cpp)

# link with boost
target_link_libraries(Database ${Boost_LIBRARIES})

# install library and main headers
install(TARGETS Database DESTINATION ${LIB_INSTALL_DIR})
#install(FILES ${libmpi2_HDRS} DESTINATION ${INCLUDE_INSTALL_DIR}/mpi2)

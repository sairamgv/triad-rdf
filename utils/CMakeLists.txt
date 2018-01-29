#include_directories(${Boost_INCLUDE_DIRS})
#link_libraries(${Boost_LIBRARIES} ${RG_LIBRARIES} mpi2 log4cxx)
#file(COPY "${D2R2_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#add_executable(FastMap FastMap.cpp)


# include sources
include_directories (${TriAD_SOURCE_DIR}/utils)

# link sources
link_directories (${TriAD_BINARY_DIR}/utils)

#link libraries
link_libraries(${Boost_LIBRARIES} mpi2 log4cxx)

file(COPY "${TriAD_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#Build libraries
add_library(Utils FastMap.cpp BitArray.cpp Utilities.cpp fastlz.cc)

#install targets
install(TARGETS Utils DESTINATION ${LIB_INSTALL_DIR})



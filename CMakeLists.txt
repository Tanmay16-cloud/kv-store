cmake_minimum_required(VERSION 3.20)

project(distributed_kv_store
    VERSION 0.1.0
    DESCRIPTION "Production-inspired distributed key-value store in C++"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(kvstore_core
    src/storage/kv_store.cpp
    src/protocol/command_parser.cpp
    src/protocol/command_executor.cpp
    src/server/tcp_server.cpp
)

target_include_directories(kvstore_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(kvstore_core PUBLIC cxx_std_20)

if(WIN32)
    target_compile_definitions(kvstore_core PUBLIC WIN32_LEAN_AND_MEAN NOMINMAX)
    target_link_libraries(kvstore_core PUBLIC ws2_32)
endif()

if(MSVC)
    target_compile_options(kvstore_core PRIVATE /W4)
else()
    target_compile_options(kvstore_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(kvstore
    src/main.cpp
)

target_link_libraries(kvstore PRIVATE kvstore_core)

include(CTest)

if(BUILD_TESTING)
    add_executable(kvstore_tests
        tests/kv_store_tests.cpp
    )

    target_link_libraries(kvstore_tests PRIVATE kvstore_core)

    add_test(NAME kvstore_tests COMMAND kvstore_tests)
endif()

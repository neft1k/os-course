add_library(
    vtpc
    STATIC
    vtpc.c
)

target_include_directories(
    vtpc
    PUBLIC
    .
)

add_executable(
    io_load_vtpc
    io_load.c
    io_load_args.c
    io_load_vtpc.c
)

target_include_directories(io_load_vtpc PRIVATE .)
target_link_libraries(io_load_vtpc PRIVATE vtpc)

add_executable(
    vtsh
    main.c
)

target_include_directories(
    vtsh
    PUBLIC
    .
)

target_link_libraries(
    vtsh
    PRIVATE
    libvtsh
)

add_executable(
    cpu_linreg
    cpu_linreg.c
)

target_include_directories(
    cpu_linreg
    PUBLIC
    .
)
